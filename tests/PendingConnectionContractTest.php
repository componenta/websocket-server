<?php

declare(strict_types=1);

use Componenta\WebSocket\Connection\PendingConnection;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocket;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\Handshake\Handshake;

final class PendingConnectionWriteSocket implements SocketInterface
{
    public string $written = '';

    /** @var list<SocketWriteResult> */
    private array $writes;

    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id = 'pending-write',
        public string $remoteAddress = '127.0.0.1:12345',
        SocketWriteResult ...$writes,
    ) {
        $this->writes = $writes;
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $result = array_shift($this->writes)
            ?? new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));

        if ($result->status === SocketWriteStatus::WRITTEN && $result->writtenBytes > 0) {
            $this->written .= substr($bytes, 0, $result->writtenBytes);
        }

        return $result;
    }

    public function close(): void {}
}

describe('WebSocket pending connection contract', function () {
    it('does not extend the absolute handshake deadline when bytes arrive', function () {
        $pending = new PendingConnection(
            new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
            1000.0,
        );

        $pending->append('GET /ws HTTP/1.1');

        expect($pending->buffer)->toBe('GET /ws HTTP/1.1')
            ->and($pending->expired(3999.0, 3000))->toBeFalse()
            ->and($pending->expired(4000.0, 3000))->toBeTrue()
            ->and($pending->expired(2000.0, 1000))->toBeTrue();

        $pending->socket->close();
    });

    it('keeps queued handshake responses until non-blocking writes complete', function () {
        $socket = new PendingConnectionWriteSocket(
            'pending-write',
            '127.0.0.1:12345',
            new SocketWriteResult(SocketWriteStatus::BLOCKED),
        );
        $pending = new PendingConnection($socket, 0.0);

        $pending->queueResponse('HTTP/1.1 101 Switching Protocols');
        $blocked = $pending->flush();
        $stillPending = $pending->needsWrite;
        $written = $pending->flush();

        expect($blocked)->toBeTrue()
            ->and($stillPending)->toBeTrue()
            ->and($pending->needsWrite)->toBeFalse()
            ->and($written)->toBeTrue()
            ->and($socket->written)->toBe('HTTP/1.1 101 Switching Protocols');
    });

    it('continues pending response writes from the previous partial offset', function () {
        $socket = new PendingConnectionWriteSocket(
            'pending-write',
            '127.0.0.1:12345',
            new SocketWriteResult(SocketWriteStatus::WRITTEN, 4),
        );
        $pending = new PendingConnection($socket, 0.0);

        $pending->queueResponse('HTTP/1.1 101 Switching Protocols');
        $firstFlush = $pending->flush();
        $stillPending = $pending->needsWrite;
        $secondFlush = $pending->flush();

        expect($firstFlush)->toBeTrue()
            ->and($stillPending)->toBeTrue()
            ->and($secondFlush)->toBeTrue()
            ->and($pending->needsWrite)->toBeFalse()
            ->and($socket->written)->toBe('HTTP/1.1 101 Switching Protocols');
    });

    it('does not allow cloning an owner of a pending response queue', function () {
        $socket = new PendingConnectionWriteSocket();
        $pending = new PendingConnection($socket, 0.0);
        $pending->queueResponse('response');

        expect(fn () => clone $pending)->toThrow(Error::class)
            ->and($pending->flush())->toBeTrue()
            ->and($socket->written)->toBe('response')
            ->and($pending->needsWrite)->toBeFalse();
    });

});
