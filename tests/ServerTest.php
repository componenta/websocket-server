<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Transport\Server;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketSelection;
use Componenta\WebSocket\Socket\SocketSelectorInterface;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Transport\WebSocketServerInterface;
use Psr\Clock\ClockInterface;
use Componenta\WebSocket\Connection\Connection;

final class ServerBackpressureClock implements ClockInterface
{
    private int $tick = 0;

    public function __construct(
        private readonly int $stepMs = 0,
    ) {}

    public function now(): DateTimeImmutable
    {
        $milliseconds = $this->tick * $this->stepMs;
        $this->tick++;

        return (new DateTimeImmutable('2026-06-06 00:00:00'))->modify("+{$milliseconds} milliseconds");
    }
}

final class ServerBackpressureSocket implements SocketInterface
{
    public string $written = '';
    public int $writeCalls = 0;
    public bool $closed = false;

    /** @var list<SocketReadResult> */
    private array $reads;

    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id,
        public string $remoteAddress,
        SocketReadResult ...$reads,
    ) {
        $this->reads = $reads;
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return array_shift($this->reads) ?? new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $this->writeCalls++;

        if ($this->writeCalls === 1) {
            return new SocketWriteResult(SocketWriteStatus::BLOCKED);
        }

        $this->written .= $bytes;

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

final class ServerPartialWriteSocket implements SocketInterface
{
    public string $written = '';
    public int $writeCalls = 0;
    public int $partialWriteCalls = 0;
    public bool $closed = false;

    /** @var list<SocketReadResult> */
    private array $reads;

    private bool $handshakeFlushed = false;

    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id,
        public string $remoteAddress,
        private readonly int $partialBytes,
        SocketReadResult ...$reads,
    ) {
        $this->reads = $reads;
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return array_shift($this->reads) ?? new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $this->writeCalls++;

        if (!$this->handshakeFlushed && str_starts_with($bytes, 'HTTP/1.1 ')) {
            $this->written .= $bytes;
            $this->handshakeFlushed = str_contains($this->written, "\r\n\r\n");

            return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
        }

        $writtenBytes = min($this->partialBytes, strlen($bytes));

        if ($writtenBytes === 0) {
            return new SocketWriteResult(SocketWriteStatus::WRITTEN);
        }

        $this->partialWriteCalls++;
        $this->written .= substr($bytes, 0, $writtenBytes);

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, $writtenBytes);
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

final class ServerScriptedSocket implements SocketInterface
{
    public string $written = '';
    public int $writeCalls = 0;
    public bool $closed = false;

    /** @var list<SocketReadResult> */
    private array $reads;

    /** @var list<SocketWriteResult> */
    private array $writes;

    public bool $eof {
        get => false;
    }

    /**
     * @param list<SocketReadResult> $reads
     * @param list<SocketWriteResult> $writes
     */
    public function __construct(
        public int|string $id,
        public string $remoteAddress,
        array $reads = [],
        array $writes = [],
    ) {
        $this->reads = $reads;
        $this->writes = $writes;
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return array_shift($this->reads) ?? new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $this->writeCalls++;
        $result = array_shift($this->writes);

        if ($result === null) {
            $this->written .= $bytes;

            return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
        }

        if ($result->status === SocketWriteStatus::WRITTEN) {
            $writtenBytes = $result->writtenBytes === 0 ? strlen($bytes) : min($result->writtenBytes, strlen($bytes));
            $this->written .= substr($bytes, 0, $writtenBytes);

            return new SocketWriteResult(SocketWriteStatus::WRITTEN, $writtenBytes, $result->reason);
        }

        return $result;
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

final class ServerBackpressureListener implements SocketListenerInterface
{
    public function __construct(
        public int|string $id,
        private ?SocketInterface $socket,
    ) {}

    public function accept(): ?SocketInterface
    {
        $socket = $this->socket;
        $this->socket = null;

        return $socket;
    }

    public function close(): void {}
}

final class ServerQueueListener implements SocketListenerInterface
{
    public bool $closed = false;

    /** @var list<SocketInterface> */
    private array $sockets;

    public function __construct(
        public int|string $id,
        SocketInterface ...$sockets,
    ) {
        $this->sockets = $sockets;
    }

    public bool $hasPending {
        get => $this->sockets !== [];
    }

    public function accept(): ?SocketInterface
    {
        return array_shift($this->sockets);
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

final readonly class ServerBackpressureListenerFactory implements SocketListenerFactoryInterface
{
    public function __construct(
        private SocketListenerInterface $listener,
    ) {}

    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
    {
        return $this->listener;
    }
}

final class ServerQueueSelector implements SocketSelectorInterface
{
    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        $readableListeners = [];
        $readable = [];
        $writable = [];

        foreach ($listeners as $listener) {
            if (!$listener instanceof ServerQueueListener || !$listener->hasPending) {
                continue;
            }

            $readableListeners[$listener->id] = $listener;
        }

        foreach ($read as $socket) {
            $readable[$socket->id] = $socket;
        }

        foreach ($write as $socket) {
            $writable[$socket->id] = $socket;
        }

        return new SocketSelection($readableListeners, $readable, $writable);
    }
}

final readonly class ServerFailingSelector implements SocketSelectorInterface
{
    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        throw new RuntimeException('select failed');
    }
}

final class ServerBackpressureSelector implements SocketSelectorInterface
{
    private int $calls = 0;

    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        $this->calls++;

        if ($this->calls === 1) {
            $listener = $this->first($listeners);

            return new SocketSelection($listener === null ? [] : [$listener->id => $listener], [], []);
        }

        if ($this->calls === 2) {
            $socket = $this->first($read);

            return new SocketSelection([], $socket === null ? [] : [$socket->id => $socket], []);
        }

        $socket = $this->first($write);

        return new SocketSelection([], [], $socket === null ? [] : [$socket->id => $socket]);
    }

    /**
     * @template T
     * @param iterable<T> $items
     * @return T|null
     */
    private function first(iterable $items): mixed
    {
        foreach ($items as $item) {
            return $item;
        }

        return null;
    }
}

final class ServerBackpressureApplication implements WebSocketApplicationInterface
{
    public ?WebSocketServerInterface $server = null;
    public int $connected = 0;
    public int $disconnected = 0;
    public ?CloseInfo $lastClose = null;

    /** @var list<WebSocketErrorContextInterface> */
    public array $errors = [];

    public function __construct(
        private readonly bool $stopOnConnected = true,
        private readonly bool $stopOnDisconnected = false,
        private readonly bool $stopOnFailed = false,
    ) {}

    public function connected(ConnectionInterface $connection): void
    {
        $this->connected++;

        if ($this->stopOnConnected) {
            $this->server?->stop();
        }
    }

    public function received(ConnectionInterface $connection, Message $message): void {}

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
    {
        $this->disconnected++;
        $this->lastClose = $close;

        if ($this->stopOnDisconnected) {
            $this->server?->stop();
        }
    }

    public function failed(WebSocketErrorContextInterface $context): void
    {
        $this->errors[] = $context;

        if ($this->stopOnFailed) {
            $this->server?->stop();
        }
    }
}

final class ServerRuntimeApplication implements WebSocketApplicationInterface
{
    public ?WebSocketServerInterface $server = null;
    public int $connected = 0;
    public int $received = 0;
    public int $disconnected = 0;
    public ?CloseInfo $lastClose = null;

    /** @var list<WebSocketErrorContextInterface> */
    public array $errors = [];

    public function __construct(
        private readonly ?string $sendOnConnected = null,
        private readonly bool $stopOnReceived = false,
        private readonly bool $stopOnDisconnected = false,
        private readonly bool $stopOnFailed = false,
    ) {}

    public function connected(ConnectionInterface $connection): void
    {
        $this->connected++;

        if ($this->sendOnConnected !== null) {
            $connection->sendText($this->sendOnConnected);
        }
    }

    public function received(ConnectionInterface $connection, Message $message): void
    {
        $this->received++;

        if ($this->stopOnReceived) {
            $this->server?->stop();
        }
    }

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
    {
        $this->disconnected++;
        $this->lastClose = $close;

        if ($this->stopOnDisconnected) {
            $this->server?->stop();
        }
    }

    public function failed(WebSocketErrorContextInterface $context): void
    {
        $this->errors[] = $context;

        if ($this->stopOnFailed) {
            $this->server?->stop();
        }
    }
}

function serverTestHandshake(string $path = '/ws', string $key = 'dGhlIHNhbXBsZSBub25jZQ=='): string
{
    return "GET {$path} HTTP/1.1\r\n"
        . "Host: example.com\r\n"
        . "Connection: Upgrade\r\n"
        . "Upgrade: websocket\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . "Sec-WebSocket-Key: {$key}\r\n"
        . "\r\n";
}

function serverTestErrorFactory(): WebSocketErrorContextFactory
{
    return new WebSocketErrorContextFactory();
}

function serverTestApplicationInvoker(): SafeWebSocketApplicationInvoker
{
    return new SafeWebSocketApplicationInvoker(serverTestErrorFactory());
}

function serverTestClientFrame(Opcode $opcode, string $payload, bool $masked = true): string
{
    return (new FrameCodec())->encode(new Frame($opcode, $payload), masked: $masked, maskKey: 'test');
}

describe('WebSocket server', function () {
    it('keeps pending handshakes alive when the first response write is blocked', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0);
        $socket = new ServerBackpressureSocket(
            'client',
            '127.0.0.1:12345',
            new SocketReadResult(
                SocketReadStatus::DATA,
                "GET /ws HTTP/1.1\r\n"
                . "Host: example.com\r\n"
                . "Connection: Upgrade\r\n"
                . "Upgrade: websocket\r\n"
                . "Sec-WebSocket-Version: 13\r\n"
                . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                . "\r\n",
            ),
        );
        $listener = new ServerBackpressureListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerBackpressureSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($socket->writeCalls)->toBeGreaterThanOrEqual(3)
            ->and($socket->written)->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->lastClose?->code)->toBe(CloseCode::GOING_AWAY)
            ->and($application->errors)->toBe([])
            ->and($socket->closed)->toBeTrue();
    });

    it('reports selector failures and closes the listener', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $listener = new ServerQueueListener('listener');
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerFailingSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::SELECT)
            ->and($application->errors[0]->error->getMessage())->toBe('select failed')
            ->and($listener->closed)->toBeTrue();
    });

    it('drains shutdown close frames through partial socket writes', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(
            path: '/ws',
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 64,
        );
        $socket = new ServerPartialWriteSocket(
            'client',
            '127.0.0.1:12345',
            2,
            new SocketReadResult(
                SocketReadStatus::DATA,
                "GET /ws HTTP/1.1\r\n"
                . "Host: example.com\r\n"
                . "Connection: Upgrade\r\n"
                . "Upgrade: websocket\r\n"
                . "Sec-WebSocket-Version: 13\r\n"
                . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                . "\r\n",
            ),
        );
        $listener = new ServerBackpressureListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerBackpressureSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($socket->written)->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($socket->written)->toContain("\x88\x11")
            ->and($socket->partialWriteCalls)->toBeGreaterThan(1)
            ->and($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->lastClose?->code)->toBe(CloseCode::GOING_AWAY)
            ->and($application->lastClose?->reason)->toBe('Server shutdown')
            ->and($application->errors)->toBe([])
            ->and($socket->closed)->toBeTrue();
    });

    it('rejects excess pending connections before reading their handshakes', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(
            path: '/ws',
            maxPendingConnections: 1,
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 0,
        );
        $accepted = new ServerScriptedSocket(
            'accepted',
            '127.0.0.1:12345',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())],
        );
        $rejected = new ServerScriptedSocket('rejected', '127.0.0.1:12346');
        $listener = new ServerQueueListener('listener', $accepted, $rejected);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->errors)->toBe([])
            ->and($accepted->written)->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($accepted->closed)->toBeTrue()
            ->and($rejected->written)->toContain('HTTP/1.1 503 Service Unavailable')
            ->and($rejected->closed)->toBeTrue();
    });

    it('counts pending handshakes against max established connection capacity', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(
            path: '/ws',
            maxConnections: 1,
            maxPendingConnections: 2,
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 0,
        );
        $accepted = new ServerScriptedSocket(
            'accepted',
            '127.0.0.1:12345',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())],
        );
        $rejected = new ServerScriptedSocket(
            'rejected',
            '127.0.0.1:12346',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake(key: 'eHh4eHh4eHh4eHh4eHh4eA=='))],
        );
        $listener = new ServerQueueListener('listener', $accepted, $rejected);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->errors)->toBe([])
            ->and($accepted->written)->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($rejected->written)->toContain('HTTP/1.1 503 Service Unavailable')
            ->and($rejected->written)->not->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($rejected->closed)->toBeTrue();
    });

    it('cleans up rejected handshakes without opening application connections', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $rejected = new ServerScriptedSocket(
            'rejected',
            '127.0.0.1:12345',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake('/wrong'))],
        );
        $accepted = new ServerScriptedSocket(
            'accepted',
            '127.0.0.1:12346',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())],
        );
        $listener = new ServerQueueListener('listener', $rejected, $accepted);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toBe([])
            ->and($rejected->written)->toContain('HTTP/1.1 404 Not Found')
            ->and($rejected->closed)->toBeTrue()
            ->and($accepted->written)->toContain('HTTP/1.1 101 Switching Protocols')
            ->and($accepted->closed)->toBeTrue();
    });

    it('reports and closes pending connections when handshake response writes fail', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket(
            'client',
            '127.0.0.1:12345',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())],
            [new SocketWriteResult(SocketWriteStatus::ERROR, reason: 'boom')],
        );
        $listener = new ServerQueueListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication(stopOnFailed: true);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(0)
            ->and($application->disconnected)->toBe(0)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::WRITE)
            ->and($socket->closed)->toBeTrue();
    });

    it('reports and closes established connections when socket writes fail', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket(
            'client',
            '127.0.0.1:12345',
            [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())],
            [
                new SocketWriteResult(SocketWriteStatus::WRITTEN),
                new SocketWriteResult(SocketWriteStatus::ERROR, reason: 'write failed'),
            ],
        );
        $listener = new ServerQueueListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerRuntimeApplication(sendOnConnected: 'hello', stopOnFailed: true);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::WRITE)
            ->and($application->lastClose?->reason)->toBe('Unable to write to socket.')
            ->and($socket->closed)->toBeTrue();
    });

    it('reports and closes established connections when socket reads fail', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket(
            'client',
            '127.0.0.1:12345',
            [
                new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
                new SocketReadResult(SocketReadStatus::ERROR, reason: 'read failed'),
            ],
        );
        $listener = new ServerQueueListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerRuntimeApplication(stopOnDisconnected: true);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::READ)
            ->and($application->lastClose?->reason)->toBe('read failed')
            ->and($socket->closed)->toBeTrue();
    });

    it('stops cleanly when the application stops inside a received callback', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 64);
        $socket = new ServerScriptedSocket(
            'client',
            '127.0.0.1:12345',
            [
                new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
                new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'hello')),
            ],
        );
        $listener = new ServerQueueListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerRuntimeApplication(stopOnReceived: true);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->received)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toBe([])
            ->and($socket->written)->toContain("\x88\x11")
            ->and($socket->closed)->toBeTrue();
    });

    it('reports protocol errors and writes a protocol close frame', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(path: '/ws', selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket(
            'client',
            '127.0.0.1:12345',
            [
                new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
                new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'bad', masked: false)),
            ],
        );
        $listener = new ServerQueueListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerQueueSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerRuntimeApplication(stopOnDisconnected: true);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::READ)
            ->and($application->lastClose?->code)->toBe(CloseCode::PROTOCOL_ERROR)
            ->and($socket->written)->toContain("\x88")
            ->and($socket->closed)->toBeTrue();
    });

    it('closes established connections when heartbeat pong timeout expires', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(
            path: '/ws',
            heartbeatIntervalMs: 1,
            pongTimeoutMs: 1,
            idleTimeoutMs: 0,
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 0,
        );
        $socket = new ServerBackpressureSocket(
            'client',
            '127.0.0.1:12345',
            new SocketReadResult(
                SocketReadStatus::DATA,
                "GET /ws HTTP/1.1\r\n"
                . "Host: example.com\r\n"
                . "Connection: Upgrade\r\n"
                . "Upgrade: websocket\r\n"
                . "Sec-WebSocket-Version: 13\r\n"
                . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                . "\r\n",
            ),
        );
        $listener = new ServerBackpressureListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerBackpressureSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication(stopOnConnected: false, stopOnDisconnected: true);
        $application->server = $server;

        $server->run($application);

        expect($socket->written)->toContain("\x89\x00")
            ->and($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->lastClose?->clean)->toBeFalse()
            ->and($application->lastClose?->reason)->toBe('WebSocket pong timeout.')
            ->and($socket->closed)->toBeTrue();
    });

    it('closes idle established connections with a close frame', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(
            path: '/ws',
            heartbeatIntervalMs: 0,
            pongTimeoutMs: 1,
            idleTimeoutMs: 1,
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 0,
        );
        $socket = new ServerBackpressureSocket(
            'client',
            '127.0.0.1:12345',
            new SocketReadResult(
                SocketReadStatus::DATA,
                "GET /ws HTTP/1.1\r\n"
                . "Host: example.com\r\n"
                . "Connection: Upgrade\r\n"
                . "Upgrade: websocket\r\n"
                . "Sec-WebSocket-Version: 13\r\n"
                . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                . "\r\n",
            ),
        );
        $listener = new ServerBackpressureListener('listener', $socket);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerBackpressureSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new ServerBackpressureApplication(stopOnConnected: false, stopOnDisconnected: true);
        $application->server = $server;

        $server->run($application);

        expect($socket->written)->toContain("\x88")
            ->and($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->lastClose?->clean)->toBeTrue()
            ->and($application->lastClose?->reason)->toBe('Idle timeout')
            ->and($socket->closed)->toBeTrue();
    });
});
