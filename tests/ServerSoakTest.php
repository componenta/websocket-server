<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
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
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Transport\WebSocketServerInterface;
use Psr\Clock\ClockInterface;
use Componenta\WebSocket\Connection\Connection;

final readonly class ServerSoakClock implements ClockInterface
{
    public function now(): DateTimeImmutable
    {
        return new DateTimeImmutable('2026-06-06 00:00:00');
    }
}

final class ServerSoakSocket implements SocketInterface
{
    public string $written = '';
    public bool $closed = false;

    /** @var list<SocketReadResult> */
    private array $reads;

    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id,
        public string $remoteAddress,
        string $handshake,
    ) {
        $this->reads = [new SocketReadResult(SocketReadStatus::DATA, $handshake)];
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return array_shift($this->reads) ?? new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $this->written .= $bytes;

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

final class ServerSoakListener implements SocketListenerInterface
{
    /** @var list<SocketInterface> */
    private array $sockets;

    public function __construct(
        public int|string $id,
        SocketInterface ...$sockets,
    ) {
        $this->sockets = $sockets;
    }

    public function hasPending(): bool
    {
        return $this->sockets !== [];
    }

    public function accept(): ?SocketInterface
    {
        return array_shift($this->sockets);
    }

    public function close(): void {}
}

final readonly class ServerSoakListenerFactory implements SocketListenerFactoryInterface
{
    public function __construct(
        private SocketListenerInterface $listener,
    ) {}

    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
    {
        return $this->listener;
    }
}

final class ServerSoakSelector implements SocketSelectorInterface
{
    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        $readableListeners = [];
        $readable = [];
        $writable = [];

        foreach ($listeners as $listener) {
            if ($listener instanceof ServerSoakListener && $listener->hasPending()) {
                $readableListeners[$listener->id] = $listener;
            }
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

final class ServerSoakApplication implements WebSocketApplicationInterface
{
    public ?WebSocketServerInterface $server = null;
    public int $connected = 0;
    public int $disconnected = 0;

    /** @var list<WebSocketErrorContextInterface> */
    public array $errors = [];

    public function __construct(
        private readonly int $expectedConnections,
    ) {}

    public function connected(ConnectionInterface $connection): void
    {
        $this->connected++;
        $connection->sendText('welcome');
        $connection->close();
    }

    public function received(ConnectionInterface $connection, Message $message): void {}

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
    {
        $this->disconnected++;

        if ($this->disconnected >= $this->expectedConnections) {
            $this->server?->stop();
        }
    }

    public function failed(WebSocketErrorContextInterface $context): void
    {
        $this->errors[] = $context;
        $this->server?->stop();
    }
}

function serverSoakHandshake(int $index): string
{
    return "GET /ws HTTP/1.1\r\n"
        . "Host: example.com\r\n"
        . "Connection: Upgrade\r\n"
        . "Upgrade: websocket\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . 'Sec-WebSocket-Key: ' . base64_encode(str_pad((string) $index, 16, 'x')) . "\r\n"
        . "\r\n";
}

describe('WebSocket server soak lifecycle', function () {
    it('cleans up repeated connection write close cycles', function () {
        $count = 32;
        $clock = new ServerSoakClock();
        $options = new WebSocketOptions(
            path: '/ws',
            maxConnections: $count,
            maxPendingConnections: $count,
            heartbeatIntervalMs: 0,
            pongTimeoutMs: 1000,
            idleTimeoutMs: 0,
            selectTimeoutUsec: 0,
            shutdownDrainTimeoutMs: 0,
        );
        $sockets = [];

        for ($i = 0; $i < $count; $i++) {
            $sockets[] = new ServerSoakSocket("client-{$i}", "127.0.0.1:" . (12000 + $i), serverSoakHandshake($i));
        }

        $listener = new ServerSoakListener('listener', ...$sockets);
        $server = new Server(
            $options,
            new ServerSoakListenerFactory($listener),
            new SelectEventLoop(new ServerSoakSelector(), $clock, $options->selectTimeoutUsec),
            new Handshake($options),
            new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock),
            new SafeWebSocketApplicationInvoker(new WebSocketErrorContextFactory()),
            new WebSocketErrorContextFactory(),
            $clock,
        );
        $application = new ServerSoakApplication($count);
        $application->server = $server;

        $server->run($application);

        expect($application->connected)->toBe($count)
            ->and($application->disconnected)->toBe($count)
            ->and($application->errors)->toBe([]);

        foreach ($sockets as $socket) {
            expect($socket->closed)->toBeTrue()
                ->and($socket->written)->toContain('HTTP/1.1 101 Switching Protocols')
                ->and($socket->written)->toContain('welcome')
                ->and($socket->written)->toContain("\x88");
        }
    });
});
