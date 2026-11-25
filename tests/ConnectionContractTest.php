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

    it('notifies the transport only when an empty queue becomes writable', function () {
        $connection = contractTestConnection();
        $notifications = 0;
        $connection->setWriteNotifier(function () use (&$notifications): void {
            $notifications++;
        });

        $connection->application->sendText('one');
        $connection->application->sendText('two');
        expect($notifications)->toBe(1);

        $connection->flush();
        $connection->application->sendText('three');
        expect($notifications)->toBe(2);

        $connection->markClosed();
        expect($connection->application->sendText('late')->accepted)->toBeFalse()
            ->and($notifications)->toBe(2)
            ->and($connection->queuedBytes)->toBe(0);
        $connection->socket->close();
    });

    it('exposes a stable application handle without transport capabilities', function () {
        $connection = contractTestConnection();
        $handle = $connection->application;
        $context = $handle->context->with('user', 42);
        $handle->context = $context;

        expect($handle)->toBe($connection->application)
            ->and($handle)->not->toBeInstanceOf(Componenta\WebSocket\Connection\TransportConnectionInterface::class)
            ->and(method_exists($handle, 'markClosed'))->toBeFalse()
            ->and(property_exists($handle, 'socket'))->toBeFalse()
            ->and($handle->id)->toBe('contract')
            ->and($connection->context->get('user'))->toBe(42);
        $connection->socket->close();
    });

    it('waits for the peer close after sending a local close frame', function () {
        $connection = contractTestConnection();
        $codec = new FrameCodec();
        $connection->close(CloseCode::NORMAL, 'done');
        $connection->flush();

        expect($connection->readyToClose)->toBeFalse()
            ->and($connection->closeInfo?->clean)->toBeFalse();

        $connection->appendBytes($codec->encode(new Frame(Opcode::CLOSE, pack('n', 1000)), true, 'mask'));

        expect($connection->readyToClose)->toBeTrue();
        $connection->markClosed();
        expect($connection->closeInfo?->clean)->toBeTrue();
        $connection->socket->close();
    });

    it('ignores data after a received close in the same socket read', function () {
        $connection = contractTestConnection();
        $codec = new FrameCodec();
        $bytes = $codec->encode(new Frame(Opcode::TEXT, 'before'), true, 'mask')
            . $codec->encode(new Frame(Opcode::CLOSE, pack('n', 1000)), true, 'mask')
            . $codec->encode(new Frame(Opcode::TEXT, 'after'), true, 'mask');

        $messages = $connection->appendBytes($bytes);

        expect(array_map(static fn (Message $message) => $message->payload, $messages))->toBe(['before'])
            ->and($connection->readyToClose)->toBeFalse();
        $connection->flush();
        expect($connection->readyToClose)->toBeTrue();
        $connection->socket->close();
    });

    it('validates the peer close payload even after sending a close', function () {
        $connection = contractTestConnection();
        $connection->close();
        $connection->flush();
        $bytes = (new FrameCodec())->encode(new Frame(Opcode::CLOSE, "\x01"), true, 'mask');

        expect(fn () => $connection->appendBytes($bytes))->toThrow(Componenta\WebSocket\Exception\ProtocolException::class)
            ->and($connection->readyToClose)->toBeFalse();
        $connection->socket->close();
    });

    it('records an abnormal transport failure instead of a clean requested close', function () {
        $connection = contractTestConnection();
        $connection->close();
        $connection->markClosed(CloseInfo::transport('write failed'));

        expect($connection->closeInfo?->code)->toBe(CloseCode::ABNORMAL)
            ->and($connection->closeInfo?->reason)->toBe('write failed')
            ->and($connection->closeInfo?->clean)->toBeFalse();
        $connection->socket->close();
    });

    it('rejects invalid UTF-8 text without queuing bytes and accepts binary bytes', function () {
        $connection = contractTestConnection();

        $text = $connection->application->sendText("\xc3\x28");
        $message = $connection->application->send(new Message(Opcode::TEXT, "\xc3\x28"));
        $binary = $connection->application->sendBinary("\xc3\x28");

        expect($text->accepted)->toBeFalse()
            ->and($text->queuedBytes)->toBe(0)
            ->and($message->accepted)->toBeFalse()
            ->and($message->queuedBytes)->toBe(0)
            ->and($binary->accepted)->toBeTrue();
        $connection->socket->close();
    });

    it('only acknowledges a fully written ping with a matching pong payload', function () {
        $connection = contractTestConnection();
        $codec = new FrameCodec();
        $matching = $codec->encode(new Frame(Opcode::PONG, 'token'), true, 'mask');
        $different = $codec->encode(new Frame(Opcode::PONG, 'wrong'), true, 'mask');

        $connection->ping('token');
        expect($connection->awaitingPong)->toBeTrue()
            ->and($connection->pingSent)->toBeFalse();

        $connection->appendBytes($matching);
        expect($connection->awaitingPong)->toBeTrue();

        $connection->flush();
        expect($connection->pingSent)->toBeTrue();

        $connection->appendBytes($different);
        expect($connection->awaitingPong)->toBeTrue();

        $connection->appendBytes($matching);
        expect($connection->awaitingPong)->toBeFalse()
            ->and($connection->pingSent)->toBeFalse();
        $connection->socket->close();
    });

    it('preserves wire order while appending during partial writes across large chunks', function () {
        $socket = new ConnectionPartialWriteSocket('partial-write', '127.0.0.1:12345',
            new SocketWriteResult(SocketWriteStatus::WRITTEN, 1),
            new SocketWriteResult(SocketWriteStatus::BLOCKED),
            new SocketWriteResult(SocketWriteStatus::WRITTEN, 10),
        );
        $codec = new FrameCodec();
        $connection = new Connection($socket, $codec, new MessageAssembler(200000), 200000, 'chunked',
            new HandshakeRequest('GET', '/ws', '/ws', []));
        $first = str_repeat('a', 140000);
        $second = "b\x00c";
        $connection->sendBinary($first);
        $connection->flush();
        $connection->sendBinary($second);

        for ($attempt = 0; $connection->needsWrite && $attempt < 20; $attempt++) {
            expect($connection->flush())->toBeTrue();
        }

        expect($connection->queuedBytes)->toBe(0)
            ->and($socket->written)->toBe(
                $codec->encode(new Frame(Opcode::BINARY, $first))
                . $codec->encode(new Frame(Opcode::BINARY, $second)),
            );
    });

    it('shares the outgoing byte limit and releases reservations on partial writes and close', function () {
        $budget = new Componenta\WebSocket\Connection\OutgoingBudget(12);
        $socket = new ConnectionPartialWriteSocket('a', '127.0.0.1:12345',
            new SocketWriteResult(SocketWriteStatus::WRITTEN, 2));
        $request = new HandshakeRequest('GET', '/ws', '/ws', []);
        $a = new Connection($socket, new FrameCodec(), new MessageAssembler(1024), 1024, 'a', $request,
            outgoingBudget: $budget);
        $b = new Connection(new ConnectionPartialWriteSocket('b'), new FrameCodec(), new MessageAssembler(1024), 1024, 'b', $request,
            outgoingBudget: $budget);

        expect($a->sendText('1234')->accepted)->toBeTrue()
            ->and($b->sendText('5678')->accepted)->toBeTrue()
            ->and($b->sendText('')->accepted)->toBeFalse();

        $a->flush();

        expect($b->sendText('')->accepted)->toBeTrue()
            ->and($b->sendText('')->accepted)->toBeFalse();

        $a->markClosed();

        expect($b->sendText('xx')->accepted)->toBeTrue()
            ->and($b->sendText('')->accepted)->toBeFalse();
        $b->markClosed();
    });

    it('does not allow duplicating the ownership of an active transport connection', function () {
        $connection = contractTestConnection();
        try {
            $connection->sendText('hello');

            expect(fn () => clone $connection)->toThrow(Error::class)
                ->and($connection->queuedBytes)->toBe(7)
                ->and($connection->flush())->toBeTrue()
                ->and($connection->queuedBytes)->toBe(0);
        } finally {
            $connection->socket->close();
        }
    });

});
