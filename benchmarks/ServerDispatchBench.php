<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Loop\MonotonicClock;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketSelection;
use Componenta\WebSocket\Socket\SocketSelectorInterface;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Transport\Server;
use PhpBench\Attributes as Bench;

/** Exercise the complete server dispatch path without kernel/network cost. */
#[Bench\Groups(['checkpoint-runtime'])]
final class ServerDispatchBench
{
    #[Bench\Revs(20)]
    public function benchEcho512ImmediatelyWritableMessages(): void
    {
        $socket = new class implements SocketInterface {
            public string $id = 'client';
            public string $remoteAddress = 'local';
            public bool $eof { get => false; }
            public int $reads = 0;
            public int $echoes = 0;
            public bool $closed = false;
            private bool $upgraded = false;
            private bool $closeWritten = false;

            public function read(int $maxBytes): SocketReadResult
            {
                $index = $this->reads++;
                if ($index === 0) {
                    return new SocketReadResult(SocketReadStatus::DATA,
                        "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
                        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
                }
                if ($index <= 512) {
                    return new SocketReadResult(SocketReadStatus::DATA, "\x81\x85mask" . ('hello' ^ 'maskm'));
                }
                if ($index === 513) {
                    return new SocketReadResult(SocketReadStatus::DATA, "\x88\x82mask" . ("\x03\xe8" ^ 'ma'));
                }
                return new SocketReadResult(SocketReadStatus::CLOSED);
            }

            public function write(string $bytes): SocketWriteResult
            {
                if (!$this->upgraded) {
                    if (!str_starts_with($bytes, "HTTP/1.1 101 ") || !str_ends_with($bytes, "\r\n\r\n")) {
                        throw new \RuntimeException('Invalid upgrade response.');
                    }
                    $this->upgraded = true;
                } elseif ($bytes === "\x81\x05hello") {
                    if ($this->closeWritten) {
                        throw new \RuntimeException('Echo after close.');
                    }
                    $this->echoes++;
                } elseif ($bytes === "\x88\x02\x03\xe8" && $this->echoes === 512) {
                    $this->closeWritten = true;
                } else {
                    throw new \RuntimeException('Incorrect echo or close frame.');
                }

                return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
            }

            public function close(): void { $this->closed = true; }
        };
        $listener = new class($socket) implements SocketListenerInterface {
            public string $id = 'listener';
            public bool $pending = true;
            public function __construct(private readonly SocketInterface $socket) {}
            public function accept(): ?SocketInterface
            {
                if (!$this->pending) {
                    return null;
                }
                $this->pending = false;
                return $this->socket;
            }
            public function close(): void {}
        };
        $factory = new class($listener) implements SocketListenerFactoryInterface {
            public function __construct(private readonly SocketListenerInterface $listener) {}
            public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
            {
                return $this->listener;
            }
        };
        $selector = new class implements SocketSelectorInterface {
            private int $polls = 0;
            public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}
            public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
            {
                if (++$this->polls > 2_000) {
                    throw new \RuntimeException('Server failed to finish the workload.');
                }
                $ready = [];
                foreach ($listeners as $listener) {
                    if ($listener->pending) {
                        $ready[$listener->id] = $listener;
                    }
                }
                return new SocketSelection(
                    $ready,
                    is_array($read) ? $read : iterator_to_array($read),
                    is_array($write) ? $write : iterator_to_array($write),
                );
            }
        };
        $options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 0, selectTimeoutUsec: 0);
        $clock = new MonotonicClock();
        $codec = new FrameCodec();
        $errors = new WebSocketErrorContextFactory();
        $server = new Server(
            $options, $factory, new SelectEventLoop($selector, $clock, 0),
            new Handshake($options), new ConnectionFactory($codec, $options, new MessageAssemblerFactory()),
            new PendingConnectionFactory($clock), new SafeWebSocketApplicationInvoker($errors), $errors, $clock,
        );
        $application = new class($server) implements WebSocketApplicationInterface {
            public int $received = 0;
            public int $errors = 0;
            public bool $clean = false;
            public function __construct(private readonly Server $server) {}
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void
            {
                if ($message->payload !== 'hello' || !$connection->send($message)->accepted) {
                    throw new \RuntimeException('Echo rejected.');
                }
                $this->received++;
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
            {
                $this->clean = $close->clean;
                $this->server->stop();
            }
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors++;
                $this->server->stop();
            }
        };

        $server->run($application);
        if ($application->errors !== 0 || $application->received !== 512
            || $socket->echoes !== 512 || !$socket->closed || !$application->clean
        ) {
            throw new \RuntimeException('Incomplete echo workload.');
        }
    }
}
