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
use Componenta\WebSocket\Loop\MonotonicClockInterface;
use Componenta\WebSocket\Connection\Connection;

final class ServerBackpressureClock implements MonotonicClockInterface
{
    private int $tick = 0;

    public function __construct(
        private readonly float $stepMs = 0.1,
    ) {}

    public function nowMilliseconds(): float
    {
        return $this->tick++ * $this->stepMs;
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
    public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}

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
    public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}

    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        throw new RuntimeException('select failed');
    }
}

final class ServerBackpressureSelector implements SocketSelectorInterface
{
    public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}

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
            ->and($application->lastClose?->initiator)->toBe(\Componenta\WebSocket\Protocol\CloseInitiator::SERVER)
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
            ->and($application->lastClose?->initiator)->toBe(\Componenta\WebSocket\Protocol\CloseInitiator::SERVER)
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

        expect($socket->written)->toContain("\x89\x08")
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
            closeHandshakeTimeoutMs: 2,
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
            ->and($application->lastClose?->clean)->toBeFalse()
            ->and($application->lastClose?->reason)->toBe('Idle timeout')
            ->and($socket->closed)->toBeTrue();
    });

    it('writes to another idle connection without waiting for an event on that connection', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, idleTimeoutMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $handshake = "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
        $codec = new FrameCodec();
        $a = new ServerScriptedSocket('a', '127.0.0.1:10001', [
            new SocketReadResult(SocketReadStatus::DATA, $handshake),
            new SocketReadResult(SocketReadStatus::DATA, $codec->encode(new Frame(Opcode::TEXT, 'relay'), true, 'mask')),
        ]);
        $b = new ServerScriptedSocket('b', '127.0.0.1:10002', [
            new SocketReadResult(SocketReadStatus::DATA, $handshake),
        ]);
        $listener = new ServerQueueListener('listener', $a, $b);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server(
            $options,
            new ServerBackpressureListenerFactory($listener),
            $loop,
            new Handshake($options),
            new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            serverTestApplicationInvoker(),
            serverTestErrorFactory(),
            $clock,
        );
        $application = new class implements WebSocketApplicationInterface {
            public array $connections = [];
            public array $errors = [];
            public function connected(ConnectionInterface $connection): void
            {
                $this->connections[$connection->remoteAddress] = $connection;
            }
            public function received(ConnectionInterface $connection, Message $message): void
            {
                $this->connections['127.0.0.1:10002']->sendText($message->payload);
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        $bytes = substr($b->written, strpos($b->written, "\r\n\r\n") + 4);
        $frame = $codec->decode($bytes, expectMasked: false);
        expect($application->errors)->toBe([])
            ->and($application->connections['127.0.0.1:10002'])->not->toBeInstanceOf(Componenta\WebSocket\Connection\TransportConnectionInterface::class)
            ->and($frame?->opcode)->toBe(Opcode::TEXT)
            ->and($frame?->payload)->toBe('relay');
    });

    it('disconnects a failed application even when its close frame does not fit in the queue', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(maxOutgoingBufferSize: 8, heartbeatIntervalMs: 0, closeHandshakeTimeoutMs: 2, selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket('client', '127.0.0.1:10001', [
            new SocketReadResult(SocketReadStatus::DATA, "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"),
        ]);
        $listener = new ServerQueueListener('listener', $socket);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory($listener), $loop,
            new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new class($server) implements WebSocketApplicationInterface {
            public ?CloseInfo $close = null;
            public array $errors = [];
            public function __construct(private WebSocketServerInterface $server) {}
            public function connected(ConnectionInterface $connection): void
            {
                $connection->sendText('123456');
                throw new RuntimeException('application failed');
            }
            public function received(ConnectionInterface $connection, Message $message): void {}
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->close = $close;
                $this->server->stop();
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        expect($application->close?->code)->toBe(CloseCode::ABNORMAL)
            ->and($application->close?->clean)->toBeFalse()
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::APPLICATION)
            ->and($socket->closed)->toBeTrue();
    });

    it('continues shutdown drain after a temporary blocked write', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(shutdownDrainTimeoutMs: 50, heartbeatIntervalMs: 0, selectTimeoutUsec: 0);
        $handshake = "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
        $codec = new FrameCodec();
        $socket = new ServerScriptedSocket('client', '127.0.0.1:10001', [
            new SocketReadResult(SocketReadStatus::DATA, $handshake),
            new SocketReadResult(SocketReadStatus::EMPTY),
            new SocketReadResult(SocketReadStatus::DATA, $codec->encode(new Frame(Opcode::CLOSE, pack('n', 1001)), true, 'mask')),
        ], [
            new SocketWriteResult(SocketWriteStatus::WRITTEN),
            new SocketWriteResult(SocketWriteStatus::BLOCKED),
            new SocketWriteResult(SocketWriteStatus::WRITTEN),
        ]);
        $listener = new ServerBackpressureListener('listener', $socket);
        $server = new Server(
            $options, new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop(new ServerBackpressureSelector(), $clock, 0),
            new Handshake($options), new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock,
        );
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        $bytes = substr($socket->written, strpos($socket->written, "\r\n\r\n") + 4);
        $frame = $codec->decode($bytes, expectMasked: false);
        expect($frame?->opcode)->toBe(Opcode::CLOSE)
            ->and($application->lastClose?->clean)->toBeTrue()
            ->and($application->lastClose?->initiator)->toBe(\Componenta\WebSocket\Protocol\CloseInitiator::SERVER)
            ->and($socket->closed)->toBeTrue()
            ->and($application->errors)->toBe([]);
    });

    it('rejects an unselectable socket without losing another admitted connection', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $request = "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
        $bad = new ServerScriptedSocket('bad', '127.0.0.1:10001');
        $good = new ServerScriptedSocket('good', '127.0.0.1:10002', [new SocketReadResult(SocketReadStatus::DATA, $request)]);
        $listener = new ServerQueueListener('listener', $bad, $good);
        $selector = new class implements SocketSelectorInterface {
            public function assertSelectable(SocketInterface|SocketListenerInterface $target): void
            {
                if ($target->id === 'bad') {
                    throw new RuntimeException('unselectable socket');
                }
            }
            public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
            {
                return (new ServerQueueSelector())->select($listeners, $read, $write, $timeoutUsec);
            }
        };
        $server = new Server($options, new ServerBackpressureListenerFactory($listener),
            new SelectEventLoop($selector, $clock, 0), new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new ServerBackpressureApplication();
        $application->server = $server;

        $server->run($application);

        expect($bad->closed)->toBeTrue()
            ->and($good->closed)->toBeTrue()
            ->and($application->connected)->toBe(1)
            ->and($application->disconnected)->toBe(1)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::ACCEPT)
            ->and($good->written)->toContain('HTTP/1.1 101 Switching Protocols');
    });

    it('delivers a message before a later close regardless of TCP read boundaries', function (bool $coalesced) {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $data = serverTestClientFrame(Opcode::TEXT, 'hello');
        $close = serverTestClientFrame(Opcode::CLOSE, pack('n', CloseCode::NORMAL));
        $reads = [new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake())];
        foreach ($coalesced ? [$data . $close] : [$data, $close] as $bytes) {
            $reads[] = new SocketReadResult(SocketReadStatus::DATA, $bytes);
        }
        $socket = new ServerScriptedSocket('client', '127.0.0.1:12345', $reads);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server(
            $options, new ServerBackpressureListenerFactory(new ServerQueueListener('listener', $socket)),
            $loop, new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock,
        );
        $application = new class($server) implements WebSocketApplicationInterface {
            public array $states = [];
            public array $sent = [];
            public array $errors = [];
            public ?CloseInfo $close = null;
            public function __construct(private WebSocketServerInterface $server) {}
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void
            {
                $this->states[] = $connection->state;
                $this->sent[] = $connection->send($message)->accepted;
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->close = $close;
                $this->server->stop();
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        $wire = substr($socket->written, strpos($socket->written, "\r\n\r\n") + 4);
        $codec = new FrameCodec();
        $message = $codec->decode($wire, expectMasked: false);
        $reply = $codec->decode($wire, expectMasked: false);

        expect($application->states)->toBe([Componenta\WebSocket\Connection\ConnectionState::OPEN])
            ->and($application->sent)->toBe([true])
            ->and($application->errors)->toBe([])
            ->and($application->close?->clean)->toBeTrue()
            ->and($message?->opcode)->toBe(Opcode::TEXT)
            ->and($message?->payload)->toBe('hello')
            ->and($reply?->opcode)->toBe(Opcode::CLOSE)
            ->and($wire)->toBe('');
    })->with([
        'coalesced' => [true],
        'separate reads' => [false],
    ]);

    it('prevents error callbacks from replacing a protocol failure with an ordinary close', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $socket = new ServerScriptedSocket('client', '127.0.0.1:12345', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
            new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'bad', masked: false)),
        ]);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server(
            $options, new ServerBackpressureListenerFactory(new ServerQueueListener('listener', $socket)),
            $loop, new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock,
        );
        $application = new class($server) implements WebSocketApplicationInterface {
            public array $accepted = [];
            public array $errors = [];
            public ?CloseInfo $close = null;
            public function __construct(private WebSocketServerInterface $server) {}
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void {}
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->close = $close;
                $this->server->stop();
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
                if ($context->connection !== null) {
                    $this->accepted[] = $context->connection->sendText('late')->accepted;
                    $this->accepted[] = $context->connection->close()->accepted;
                }
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        $wire = substr($socket->written, strpos($socket->written, "\r\n\r\n") + 4);
        $reply = (new FrameCodec())->decode($wire, expectMasked: false);

        expect($application->accepted)->toBe([false, false])
            ->and($application->errors)->toHaveCount(1)
            ->and($application->close?->code)->toBe(CloseCode::PROTOCOL_ERROR)
            ->and($application->close?->clean)->toBeFalse()
            ->and($reply?->opcode)->toBe(Opcode::CLOSE)
            ->and(unpack('n', substr($reply->payload, 0, 2))[1])->toBe(CloseCode::PROTOCOL_ERROR)
            ->and($wire)->toBe('');
    });

    it('isolates a connection that exhausts the shared incoming buffer budget', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(
            heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0,
            maxTotalIncomingBufferSize: 24,
        );
        $codec = new FrameCodec();
        $a = new ServerScriptedSocket('a', '127.0.0.1:10001', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
            new SocketReadResult(SocketReadStatus::DATA, $codec->encode(new Frame(Opcode::BINARY, str_repeat('a', 16), false), true, 'mask')),
            new SocketReadResult(SocketReadStatus::DATA, $codec->encode(new Frame(Opcode::CONTINUATION), true, 'mask')),
        ]);
        $b = new ServerScriptedSocket('b', '127.0.0.1:10002', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
            new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'blocked')),
        ]);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory(new ServerQueueListener('listener', $a, $b)),
            $loop, new Handshake($options), new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new class implements WebSocketApplicationInterface {
            public array $messages = [];
            public array $closes = [];
            public array $errors = [];
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void
            {
                $this->messages[$connection->remoteAddress] = $message->payload;
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->closes[$connection->remoteAddress] = $close;
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        expect($application->messages)->toBe(['127.0.0.1:10001' => str_repeat('a', 16)])
            ->and($application->closes['127.0.0.1:10002']->code)->toBe(CloseCode::PAYLOAD_TOO_LARGE)
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->connection?->remoteAddress)->toBe('127.0.0.1:10002')
            ->and($a->closed)->toBeTrue()
            ->and($b->closed)->toBeTrue();
    });


    it('isolates write watcher registration failure to the target connection', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $codec = new FrameCodec();
        $a = new ServerScriptedSocket('a', '127.0.0.1:10001', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
            new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'relay')),
            new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'still open')),
        ]);
        $b = new ServerScriptedSocket('b', '127.0.0.1:10002', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
        ]);
        $loop = new class(new SelectEventLoop(new ServerQueueSelector(), $clock, 0)) implements \Componenta\WebSocket\Loop\EventLoopInterface {
            public function __construct(private readonly SelectEventLoop $delegate) {}
            public function onReadable(SocketInterface|SocketListenerInterface $socket, callable $callback): \Componenta\WebSocket\Loop\LoopWatcherInterface
            {
                return $this->delegate->onReadable($socket, $callback);
            }
            public function onWritable(SocketInterface $socket, callable $callback): \Componenta\WebSocket\Loop\LoopWatcherInterface
            {
                if ($socket->id === 'b') {
                    throw new RuntimeException('Unable to register B writer.');
                }
                return $this->delegate->onWritable($socket, $callback);
            }
            public function delay(int $milliseconds, callable $callback): \Componenta\WebSocket\Loop\LoopTimerInterface
            {
                return $this->delegate->delay($milliseconds, $callback);
            }
            public function defer(callable $callback): void
            {
                $this->delegate->defer($callback);
            }
            public function run(): void
            {
                $this->delegate->run();
            }
            public function stop(): void
            {
                $this->delegate->stop();
            }
        };
        $server = new Server(
            $options, new ServerBackpressureListenerFactory(new ServerQueueListener('listener', $a, $b)),
            $loop, new Handshake($options), new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock,
        );
        $application = new class implements WebSocketApplicationInterface {
            public array $connections = [];
            public array $messages = [];
            public array $errors = [];
            public array $closes = [];
            public ?bool $accepted = null;
            public function connected(ConnectionInterface $connection): void
            {
                $this->connections[$connection->remoteAddress] = $connection;
            }
            public function received(ConnectionInterface $connection, Message $message): void
            {
                $this->messages[] = $message->payload;
                if ($message->payload === 'relay') {
                    $this->accepted = $this->connections['127.0.0.1:10002']->sendText('payload')->accepted;
                }
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->closes[$connection->remoteAddress] = $close;
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        expect($application->messages)->toBe(['relay', 'still open'])
            ->and($application->accepted)->toBeFalse()
            ->and($application->errors)->toHaveCount(1)
            ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::WRITE)
            ->and($application->errors[0]->connection?->remoteAddress)->toBe('127.0.0.1:10002')
            ->and($application->closes['127.0.0.1:10002']->code)->toBe(CloseCode::ABNORMAL)
            ->and($application->closes['127.0.0.1:10001']->code)->toBe(CloseCode::GOING_AWAY)
            ->and($a->closed)->toBeTrue()
            ->and($b->closed)->toBeTrue();
    });


    it('closes the listener before draining established connections during shutdown', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 100);
        $socket = new class implements SocketInterface {
            public string $id = 'shutdown-client';
            public string $remoteAddress = 'local';
            public bool $eof { get => false; }
            public ?ServerQueueListener $listener = null;
            public bool $closed = false;
            public ?bool $listenerClosedDuringWrite = null;
            private bool $handshakeRead = false;
            private bool $acknowledged = false;
            public function read(int $maxBytes): SocketReadResult
            {
                if (!$this->handshakeRead) {
                    $this->handshakeRead = true;
                    return new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake());
                }
                if (!$this->acknowledged && $this->listenerClosedDuringWrite !== null) {
                    $this->acknowledged = true;
                    return new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::CLOSE, pack('n', 1001)));
                }
                return new SocketReadResult(SocketReadStatus::EMPTY);
            }
            public function write(string $bytes): SocketWriteResult
            {
                if (!str_starts_with($bytes, 'HTTP/1.1 ')) {
                    $this->listenerClosedDuringWrite = $this->listener?->closed;
                }
                return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
            }
            public function close(): void
            {
                $this->closed = true;
            }
        };
        $listener = new ServerQueueListener('listener', $socket);
        $socket->listener = $listener;
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory($listener), $loop,
            new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new class($server) implements WebSocketApplicationInterface {
            public ?CloseInfo $close = null;
            public array $errors = [];
            public function __construct(private readonly WebSocketServerInterface $server) {}
            public function connected(ConnectionInterface $connection): void
            {
                $this->server->stop();
            }
            public function received(ConnectionInterface $connection, Message $message): void {}
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->close = $close;
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };

        $server->run($application);

        expect($socket->listenerClosedDuringWrite)->toBeTrue()
            ->and($application->errors)->toBe([])
            ->and($application->close?->clean)->toBeTrue()
            ->and($listener->closed)->toBeTrue()
            ->and($socket->closed)->toBeTrue();
    });


    it('does not stop an unrelated event loop when the server is not running', function () {
        $clock = new ServerBackpressureClock();
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, selectTimeoutUsec: 0);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server(
            $options, new ServerBackpressureListenerFactory(new ServerQueueListener('unused')),
            $loop, new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock,
        );
        $calls = [];
        $loop->defer(function () use ($server, &$calls): void {
            $calls[] = 'stop inactive server';
            $server->stop();
        });
        $loop->defer(function () use (&$calls): void {
            $calls[] = 'unrelated task';
        });

        $loop->run();

        expect($calls)->toBe(['stop inactive server', 'unrelated task']);
    });


    it('registers a writer for a reply only if the immediate flush leaves queued bytes', function (bool $blocked) {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $codec = new FrameCodec();
        $writes = $blocked
            ? [new SocketWriteResult(SocketWriteStatus::WRITTEN), new SocketWriteResult(SocketWriteStatus::BLOCKED)]
            : [];
        $socket = new ServerScriptedSocket('client', 'local', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
            new SocketReadResult(SocketReadStatus::DATA, serverTestClientFrame(Opcode::TEXT, 'hello')),
        ], $writes);
        $selector = new class implements SocketSelectorInterface {
            private int $readEvents = 0;
            public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}
            public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
            {
                $readyListeners = [];
                $readable = [];
                $writable = [];
                foreach ($listeners as $listener) {
                    if ($listener instanceof ServerQueueListener && $listener->hasPending) {
                        $readyListeners[$listener->id] = $listener;
                    }
                }
                foreach ($read as $socket) {
                    if ($this->readEvents < 2) {
                        $readable[$socket->id] = $socket;
                        $this->readEvents++;
                    }
                }
                foreach ($write as $socket) {
                    $writable[$socket->id] = $socket;
                }
                return new SocketSelection($readyListeners, $readable, $writable);
            }
        };
        $loop = new class(new SelectEventLoop($selector, $clock, 0)) implements \Componenta\WebSocket\Loop\EventLoopInterface {
            public int $writeRegistrations = 0;
            public function __construct(private readonly SelectEventLoop $delegate) {}
            public function onReadable(SocketInterface|SocketListenerInterface $socket, callable $callback): \Componenta\WebSocket\Loop\LoopWatcherInterface
            {
                return $this->delegate->onReadable($socket, $callback);
            }
            public function onWritable(SocketInterface $socket, callable $callback): \Componenta\WebSocket\Loop\LoopWatcherInterface
            {
                $this->writeRegistrations++;
                return $this->delegate->onWritable($socket, $callback);
            }
            public function delay(int $milliseconds, callable $callback): \Componenta\WebSocket\Loop\LoopTimerInterface
            {
                return $this->delegate->delay($milliseconds, $callback);
            }
            public function defer(callable $callback): void { $this->delegate->defer($callback); }
            public function run(): void { $this->delegate->run(); }
            public function stop(): void { $this->delegate->stop(); }
        };
        $server = new Server($options, new ServerBackpressureListenerFactory(new ServerQueueListener('listener', $socket)),
            $loop, new Handshake($options), new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new class implements WebSocketApplicationInterface {
            public ?bool $accepted = null;
            public array $errors = [];
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void
            {
                $this->accepted = $connection->sendText('echo:' . $message->payload)->accepted;
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}
            public function failed(WebSocketErrorContextInterface $context): void { $this->errors[] = $context; }
        };

        $loop->delay(100, fn () => $server->stop());
        $server->run($application);

        $bytes = substr($socket->written, strpos($socket->written, "\r\n\r\n") + 4);
        expect($application->accepted)->toBeTrue()
            ->and($application->errors)->toBe([])
            ->and($codec->decode($bytes, expectMasked: false)?->payload)->toBe('echo:hello')
            ->and($bytes)->toBe('')
            ->and($loop->writeRegistrations)->toBe($blocked ? 1 : 0)
            ->and($socket->closed)->toBeTrue();
    })->with(['immediately writable' => false, 'initially blocked' => true]);


    it('reports pending socket read errors without treating an ordinary peer disconnect as a failure', function (bool $error) {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket('pending', 'local', [
            $error
                ? new SocketReadResult(SocketReadStatus::ERROR, reason: 'handshake read failed')
                : new SocketReadResult(SocketReadStatus::CLOSED),
        ]);
        $listener = new ServerQueueListener('listener', $socket);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory($listener), $loop,
            new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new ServerRuntimeApplication();
        $application->server = $server;
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        expect($application->connected)->toBe(0)
            ->and($application->disconnected)->toBe(0)
            ->and($socket->closed)->toBeTrue()
            ->and($socket->written)->toBe('')
            ->and($application->errors)->toHaveCount($error ? 1 : 0);
        if ($error) {
            expect($application->errors[0]->phase)->toBe(WebSocketErrorPhase::HANDSHAKE)
                ->and($application->errors[0]->error->getMessage())->toBe('handshake read failed')
                ->and($application->errors[0]->connection)->toBeNull();
        }
    })->with(['read error' => true, 'normal EOF' => false]);


    it('expires blocked handshake responses without announcing an application connection', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, selectTimeoutUsec: 0,
            pendingHandshakeTimeoutMs: 30, shutdownDrainTimeoutMs: 0);
        $blocked = array_fill(0, 100, new SocketWriteResult(SocketWriteStatus::BLOCKED));
        $socket = new ServerScriptedSocket('pending', 'local', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
        ], $blocked);
        $listener = new ServerQueueListener('listener', $socket);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory($listener), $loop,
            new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new ServerRuntimeApplication();
        $application->server = $server;
        $loop->delay(200, fn () => $server->stop());

        $server->run($application);

        expect($socket->writeCalls)->toBeGreaterThan(0)
            ->and($socket->writeCalls)->toBeLessThan(100)
            ->and($socket->closed)->toBeTrue()
            ->and($socket->written)->toBe('')
            ->and($application->connected)->toBe(0)
            ->and($application->disconnected)->toBe(0)
            ->and($application->errors)->toBe([]);
    });

    it('rejects a pending request beyond its handshake size limit before application activation', function () {
        $clock = new ServerBackpressureClock(stepMs: 1);
        $options = new WebSocketOptions(maxPendingHandshakeBytes: 32, heartbeatIntervalMs: 0,
            selectTimeoutUsec: 0, shutdownDrainTimeoutMs: 0);
        $socket = new ServerScriptedSocket('pending', 'local', [
            new SocketReadResult(SocketReadStatus::DATA, serverTestHandshake()),
        ]);
        $listener = new ServerQueueListener('listener', $socket);
        $loop = new SelectEventLoop(new ServerQueueSelector(), $clock, 0);
        $server = new Server($options, new ServerBackpressureListenerFactory($listener), $loop,
            new Handshake($options), new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), serverTestApplicationInvoker(), serverTestErrorFactory(), $clock);
        $application = new ServerRuntimeApplication();
        $application->server = $server;
        $loop->delay(100, fn () => $server->stop());

        $server->run($application);

        expect($socket->written)->toStartWith("HTTP/1.1 431 ")
            ->and($socket->closed)->toBeTrue()
            ->and($application->connected)->toBe(0)
            ->and($application->disconnected)->toBe(0)
            ->and($application->errors)->toBe([]);
    });

});
