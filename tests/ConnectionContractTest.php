<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInitiator;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Connection\ConnectionContext;
use Componenta\WebSocket\Connection\ConnectionState;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\FrameCodecInterface;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocket;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Frame;

function contractTestConnection(int $maxOutgoingBufferSize = 1024): Connection
{
    return new Connection(
        socket: new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
        codec: new FrameCodec(),
        assembler: new MessageAssembler(1024),
        maxPayloadSize: 1024,
        id: 'contract',
        request: new HandshakeRequest('GET', '/ws', '/ws', []),
        maxOutgoingBufferSize: $maxOutgoingBufferSize,
    );
}

final class RejectBeforeEncodeFrameCodec implements FrameCodecInterface
{
    public bool $encoded = false;

    public function encodedSize(Componenta\WebSocket\Protocol\Frame $frame, bool $masked = false): int
    {
        return 4096;
    }

    public function encode(Componenta\WebSocket\Protocol\Frame $frame, bool $masked = false, ?string $maskKey = null): string
    {
        $this->encoded = true;

        throw new RuntimeException('Frame should not be encoded.');
    }

    public function decode(string &$buffer, bool $expectMasked = true, int $maxPayloadSize = 1_048_576): ?Componenta\WebSocket\Protocol\Frame
    {
        return null;
    }
}

final class ConnectionPartialWriteSocket implements SocketInterface
{
    public string $written = '';

    /** @var list<SocketWriteResult> */
    private array $writes;

    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id = 'partial-write',
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

describe('WebSocket connection contract', function () {
    it('returns send results instead of hiding backpressure as a side effect', function () {
        $connection = contractTestConnection(maxOutgoingBufferSize: 8);

        $accepted = $connection->sendText('hi');
        $rejected = $connection->sendText('this payload does not fit');

        expect($accepted->accepted)->toBeTrue()
            ->and($accepted->queuedBytes)->toBeGreaterThan(0)
            ->and($rejected->accepted)->toBeFalse()
            ->and($rejected->reason)->toBe('Outgoing WebSocket buffer is full.');

        $connection->socket->close();
    });

    it('rejects oversized outgoing frames before encoding them', function () {
        $codec = new RejectBeforeEncodeFrameCodec();
        $connection = new Connection(
            socket: new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'contract',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
            maxOutgoingBufferSize: 8,
        );

        $result = $connection->sendText('hi');

        expect($result->accepted)->toBeFalse()
            ->and($result->reason)->toBe('Outgoing WebSocket buffer is full.')
            ->and($codec->encoded)->toBeFalse();

        $connection->socket->close();
    });

    it('tracks queued bytes across partial writes without losing the payload', function () {
        $socket = new ConnectionPartialWriteSocket(
            'partial-write',
            '127.0.0.1:12345',
            new SocketWriteResult(SocketWriteStatus::WRITTEN, 2),
        );
        $connection = new Connection(
            socket: $socket,
            codec: new FrameCodec(),
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'contract',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );

        $queued = $connection->sendText('hello')->queuedBytes;
        $firstFlush = $connection->flush();
        $remaining = $connection->queuedBytes;
        $secondFlush = $connection->flush();

        expect($firstFlush)->toBeTrue()
            ->and($remaining)->toBe($queued - 2)
            ->and($secondFlush)->toBeTrue()
            ->and($connection->queuedBytes)->toBe(0)
            ->and($connection->needsWrite)->toBeFalse()
            ->and($socket->written)->toContain('hello');
    });

    it('uses immutable connection context for framework layers', function () {
        $connection = contractTestConnection();
        $original = $connection->context;
        $next = $original->with('actor_id', 10);

        $connection->context = $next;

        expect($original)->toBeInstanceOf(ConnectionContext::class)
            ->and($original->has('actor_id'))->toBeFalse()
            ->and($next)->not->toBe($original)
            ->and($connection->context->get('actor_id'))->toBe(10)
            ->and($connection->context->without('actor_id')->has('actor_id'))->toBeFalse()
            ->and($connection->context->has('actor_id'))->toBeTrue()
            ->and($connection->context->toArray())->toBe(['actor_id' => 10]);

        $connection->socket->close();
    });

    it('returns stored null values from connection context', function () {
        $context = (new ConnectionContext())->with('actor_id', null);

        expect($context->has('actor_id'))->toBeTrue()
            ->and($context->get('actor_id', 'fallback'))->toBeNull();
    });

    it('records close metadata and rejects data writes while closing', function () {
        $connection = contractTestConnection();

        $close = $connection->close(CloseCode::NORMAL, 'done');
        $send = $connection->send(new Message(Opcode::TEXT, 'late'));

        expect($close->accepted)->toBeTrue()
            ->and($connection->state)->toBe(ConnectionState::CLOSING)
            ->and($connection->closeInfo?->initiator)->toBe(CloseInitiator::LOCAL)
            ->and($connection->closeInfo?->reason)->toBe('done')
            ->and($send->accepted)->toBeFalse()
            ->and($send->reason)->toBe('Connection is closing.');

        $connection->socket->close();
    });

    it('queues ping frames and records received pong frames', function () {
        $codec = new FrameCodec();
        $connection = new Connection(
            socket: new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'contract',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $pong = $codec->encode(new Componenta\WebSocket\Protocol\Frame(Opcode::PONG), masked: true, maskKey: 'mask');

        $ping = $connection->ping();
        $messages = $connection->appendBytes($pong);

        expect($ping->accepted)->toBeTrue()
            ->and($connection->queuedBytes)->toBe(2)
            ->and($messages)->toBe([])
            ->and($connection->receivedPongs)->toBe(1);

        $connection->socket->close();
    });

    it('does not enter closing state when the close frame cannot be queued', function () {
        $connection = contractTestConnection(maxOutgoingBufferSize: 1);

        $close = $connection->close(CloseCode::NORMAL, 'done');

        expect($close->accepted)->toBeFalse()
            ->and($connection->state)->toBe(ConnectionState::OPEN)
            ->and($connection->closeInfo)->toBeNull();

        $connection->socket->close();
    });

    it('does not enter closing state when a remote close echo cannot be queued', function () {
        $codec = new FrameCodec();
        $connection = new Connection(
            socket: new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'contract',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
            maxOutgoingBufferSize: 1,
        );
        $bytes = $codec->encode(new Componenta\WebSocket\Protocol\Frame(Opcode::CLOSE, pack('n', CloseCode::NORMAL)), masked: true, maskKey: 'mask');

        expect(fn() => $connection->appendBytes($bytes))->toThrow(RuntimeException::class)
            ->and($connection->state)->toBe(ConnectionState::OPEN)
            ->and($connection->closeInfo?->initiator)->toBe(CloseInitiator::TRANSPORT);

        $connection->socket->close();
    });
});
