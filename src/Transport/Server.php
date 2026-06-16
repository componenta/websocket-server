<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Exception\ProtocolException;
use Psr\Clock\ClockInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\WebSocketApplicationInvokerInterface;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Connection\ConnectionFactoryInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactoryInterface;
use Componenta\WebSocket\Connection\PendingConnectionInterface;
use Componenta\WebSocket\Connection\TransportConnectionInterface;
use Componenta\WebSocket\Loop\EventLoopInterface;
use Componenta\WebSocket\Loop\LoopTimerInterface;
use Componenta\WebSocket\Loop\LoopWatcherInterface;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Handshake\HandshakeInterface;
use Componenta\WebSocket\Protocol\Handshake\HandshakeResponse;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteStatus;

final class Server implements WebSocketServerInterface
{
    private bool $running = false;

    /** @var array<int|string, float> */
    private array $lastActivityAt = [];

    /** @var array<int|string, float> */
    private array $awaitingPongSince = [];

    public function __construct(
        private readonly WebSocketOptionsInterface $options,
        private readonly SocketListenerFactoryInterface $listeners,
        private readonly EventLoopInterface $loop,
        private readonly HandshakeInterface $handshake,
        private readonly ConnectionFactoryInterface $connections,
        private readonly PendingConnectionFactoryInterface $pendingConnections,
        private readonly WebSocketApplicationInvokerInterface $applications,
        private readonly WebSocketErrorContextFactoryInterface $errors,
        private readonly ClockInterface $clock,
    ) {}

    public function run(WebSocketApplicationInterface $application): void
    {
        $listener = $this->listeners->listen($this->options);
        $pending = [];
        $connections = [];
        $pendingWatchers = [];
        $pendingWriteWatchers = [];
        $pendingResponses = [];
        $readWatchers = [];
        $writeWatchers = [];
        $listenerWatcher = null;
        $cleanupTimer = null;
        $lifecycleTimer = null;
        $this->lastActivityAt = [];
        $this->awaitingPongSince = [];
        $this->running = true;

        try {
            $listenerWatcher = $this->loop->onReadable(
                $listener,
                function () use (
                    $listener,
                    &$pending,
                    &$connections,
                    &$pendingWatchers,
                    &$pendingWriteWatchers,
                    &$pendingResponses,
                    &$readWatchers,
                    &$writeWatchers,
                    $application,
                ): void {
                    $this->acceptConnections(
                        $listener,
                        $pending,
                        $connections,
                        $pendingWatchers,
                        $pendingWriteWatchers,
                        $pendingResponses,
                        $readWatchers,
                        $writeWatchers,
                        $application,
                    );
                },
            );
            $this->schedulePendingCleanup($pending, $pendingWatchers, $pendingWriteWatchers, $pendingResponses, $cleanupTimer);
            $this->scheduleConnectionLifecycle($connections, $readWatchers, $writeWatchers, $lifecycleTimer, $application);
            $this->loop->run();
        } catch (\Throwable $e) {
            $this->fail($application, WebSocketErrorPhase::SELECT, null, $e);
        } finally {
            $this->running = false;
            $listenerWatcher?->cancel();
            $cleanupTimer?->cancel();
            $lifecycleTimer?->cancel();
            $this->cancelWatchers($pendingWatchers);
            $this->cancelWatchers($pendingWriteWatchers);
            $this->cancelWatchers($readWatchers);
            $this->cancelWatchers($writeWatchers);

            $this->drainConnectionsForShutdown($connections, $readWatchers, $writeWatchers, $application);

            foreach ($pending as $connection) {
                $connection->socket->close();
            }

            $this->lastActivityAt = [];
            $this->awaitingPongSince = [];
            $listener->close();
        }
    }

    public function stop(): void
    {
        $this->running = false;
        $this->loop->stop();
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     */
    private function schedulePendingCleanup(
        array &$pending,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        ?LoopTimerInterface &$cleanupTimer,
    ): void {
        $cleanupTimer = $this->loop->delay(
            $this->options->pendingHandshakeTimeoutMs,
            function () use (&$pending, &$pendingWatchers, &$pendingWriteWatchers, &$pendingResponses, &$cleanupTimer): void {
                $this->closeExpiredPending($pending, $pendingWatchers, $pendingWriteWatchers, $pendingResponses);

                if ($this->running) {
                    $this->schedulePendingCleanup(
                        $pending,
                        $pendingWatchers,
                        $pendingWriteWatchers,
                        $pendingResponses,
                        $cleanupTimer,
                    );
                }
            },
        );
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function scheduleConnectionLifecycle(
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        ?LoopTimerInterface &$lifecycleTimer,
        WebSocketApplicationInterface $application,
    ): void {
        $intervalMs = $this->connectionLifecycleIntervalMs();

        if ($intervalMs === null) {
            return;
        }

        $lifecycleTimer = $this->loop->delay(
            $intervalMs,
            function () use (&$connections, &$readWatchers, &$writeWatchers, &$lifecycleTimer, $application): void {
                $this->runConnectionLifecycle($connections, $readWatchers, $writeWatchers, $application);

                if ($this->running) {
                    $this->scheduleConnectionLifecycle($connections, $readWatchers, $writeWatchers, $lifecycleTimer, $application);
                }
            },
        );
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function runConnectionLifecycle(
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        $now = $this->nowMs();

        foreach ($connections as $id => $connection) {
            if ($connection->closed) {
                continue;
            }

            $lastActivityAt = $this->lastActivityAt[$id] ?? $now;
            $idleMs = $now - $lastActivityAt;

            if ($this->options->pongTimeoutMs > 0
                && isset($this->awaitingPongSince[$id])
                && $now - $this->awaitingPongSince[$id] >= $this->options->pongTimeoutMs
            ) {
                $this->closeConnection(
                    $connection,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                    CloseInfo::transport('WebSocket pong timeout.'),
                );
                continue;
            }

            if ($this->options->idleTimeoutMs > 0
                && $idleMs >= $this->options->idleTimeoutMs
                && !$connection->closing
            ) {
                $this->requestConnectionClose(
                    $connection,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                    CloseCode::NORMAL,
                    'Idle timeout',
                    'Unable to queue idle timeout close frame.',
                );
                continue;
            }

            if ($this->options->heartbeatIntervalMs < 1
                || $idleMs < $this->options->heartbeatIntervalMs
                || isset($this->awaitingPongSince[$id])
                || $connection->closing
            ) {
                continue;
            }

            $result = $connection->ping();

            if (!$result->accepted) {
                $this->closeConnection(
                    $connection,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                    CloseInfo::transport('Unable to queue WebSocket ping frame.'),
                );
                continue;
            }

            $this->awaitingPongSince[$id] = $now;
            $this->flushOrWatchConnection($connection, $connections, $readWatchers, $writeWatchers, $application);
        }
    }

    private function connectionLifecycleIntervalMs(): ?int
    {
        $intervals = [];

        if ($this->options->heartbeatIntervalMs > 0) {
            $intervals[] = $this->options->heartbeatIntervalMs;
            $intervals[] = $this->options->pongTimeoutMs;
        }

        if ($this->options->idleTimeoutMs > 0) {
            $intervals[] = $this->options->idleTimeoutMs;
        }

        return $intervals === [] ? null : max(1, min($intervals));
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function acceptConnections(
        SocketListenerInterface $listener,
        array &$pending,
        array &$connections,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        try {
            while (($socket = $listener->accept()) !== null) {
                if (count($pending) >= $this->options->maxPendingConnections
                    || count($pending) + count($connections) >= $this->options->maxConnections
                ) {
                    $this->writeImmediate($socket, $this->httpClose(503, 'Service Unavailable'));
                    $socket->close();
                    continue;
                }

                $connection = $this->pendingConnections->create($socket);
                $pending[$socket->id] = $connection;
                $pendingWatchers[$socket->id] = $this->loop->onReadable(
                    $socket,
                    function () use (
                        $socket,
                        &$pending,
                        &$connections,
                        &$pendingWatchers,
                        &$pendingWriteWatchers,
                        &$pendingResponses,
                        &$readWatchers,
                        &$writeWatchers,
                        $application,
                    ): void {
                        $this->processPending(
                            $socket->id,
                            $pending,
                            $connections,
                            $pendingWatchers,
                            $pendingWriteWatchers,
                            $pendingResponses,
                            $readWatchers,
                            $writeWatchers,
                            $application,
                        );
                    },
                );
            }
        } catch (\Throwable $e) {
            $this->fail($application, WebSocketErrorPhase::ACCEPT, null, $e);
        }
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     */
    private function closeExpiredPending(
        array &$pending,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
    ): void
    {
        $now = $this->clock->now();

        foreach ($pending as $id => $connection) {
            if (!$connection->expired($now, $this->options->pendingHandshakeTimeoutMs)) {
                continue;
            }

            $this->removePending($id, $pending, $pendingWatchers, $pendingWriteWatchers, $pendingResponses, closeSocket: true);
        }
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function processPending(
        int|string $id,
        array &$pending,
        array &$connections,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        $connection = $pending[$id] ?? null;

        if ($connection === null) {
            return;
        }

        try {
            $read = $connection->socket->read(8192);

            if ($read->status === SocketReadStatus::ERROR || $read->status === SocketReadStatus::CLOSED) {
                $this->removePending($id, $pending, $pendingWatchers, $pendingWriteWatchers, $pendingResponses, closeSocket: true);
                return;
            }

            if ($read->status === SocketReadStatus::EMPTY) {
                return;
            }

            $connection->append($read->bytes, $this->clock->now());

            if ($connection->tooLarge($this->options->maxPendingHandshakeBytes)) {
                $this->queuePendingResponse(
                    $id,
                    $connection,
                    new HandshakeResponse(false, 431, $this->httpClose(431, 'Request Header Fields Too Large')),
                    $pendingWatchers,
                    $pendingWriteWatchers,
                    $pendingResponses,
                    $pending,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                );
                return;
            }

            if (!$connection->ready) {
                return;
            }

            $response = $this->handshake->accept($connection->buffer);

            $this->queuePendingResponse(
                $id,
                $connection,
                $response,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                $pending,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
            );
        } catch (\Throwable $e) {
            $this->removePending($id, $pending, $pendingWatchers, $pendingWriteWatchers, $pendingResponses, closeSocket: true);
            $this->fail($application, WebSocketErrorPhase::HANDSHAKE, null, $e);
        }
    }

    /**
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function queuePendingResponse(
        int|string $id,
        PendingConnectionInterface $connection,
        HandshakeResponse $response,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        array &$pending,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        ($pendingWatchers[$id] ?? null)?->cancel();
        unset($pendingWatchers[$id]);

        $pendingResponses[$id] = $response;
        $connection->queueResponse($response->response);
        $this->flushPending(
            $id,
            $pending,
            $connections,
            $pendingWatchers,
            $pendingWriteWatchers,
            $pendingResponses,
            $readWatchers,
            $writeWatchers,
            $application,
        );
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function ensurePendingWriteWatcher(
        int|string $id,
        PendingConnectionInterface $connection,
        array &$pending,
        array &$connections,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        if (!$connection->needsWrite || isset($pendingWriteWatchers[$id])) {
            return;
        }

        $pendingWriteWatchers[$id] = $this->loop->onWritable(
            $connection->socket,
            function () use (
                $id,
                &$pending,
                &$connections,
                &$pendingWatchers,
                &$pendingWriteWatchers,
                &$pendingResponses,
                &$readWatchers,
                &$writeWatchers,
                $application,
            ): void {
                $this->flushPending(
                    $id,
                    $pending,
                    $connections,
                    $pendingWatchers,
                    $pendingWriteWatchers,
                    $pendingResponses,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                );
            },
        );
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function flushPending(
        int|string $id,
        array &$pending,
        array &$connections,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        $connection = $pending[$id] ?? null;

        if ($connection === null) {
            ($pendingWriteWatchers[$id] ?? null)?->cancel();
            unset($pendingWriteWatchers[$id], $pendingResponses[$id]);
            return;
        }

        try {
            $flushed = $connection->flush();
        } catch (\Throwable $e) {
            $this->fail($application, WebSocketErrorPhase::WRITE, null, $e);
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            return;
        }

        if (!$flushed) {
            $this->fail(
                $application,
                WebSocketErrorPhase::WRITE,
                null,
                new \RuntimeException('Unable to write WebSocket handshake response.'),
            );
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            return;
        }

        if ($connection->needsWrite) {
            $this->ensurePendingWriteWatcher(
                $id,
                $connection,
                $pending,
                $connections,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                $readWatchers,
                $writeWatchers,
                $application,
            );
            return;
        }

        ($pendingWriteWatchers[$id] ?? null)?->cancel();
        unset($pendingWriteWatchers[$id]);

        $response = $pendingResponses[$id] ?? null;

        if ($response === null) {
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            $this->fail(
                $application,
                WebSocketErrorPhase::HANDSHAKE,
                null,
                new \RuntimeException('Pending WebSocket handshake response was lost.'),
            );
            return;
        }

        if (!$response->accepted) {
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            return;
        }

        if ($response->request === null) {
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            $this->fail(
                $application,
                WebSocketErrorPhase::HANDSHAKE,
                null,
                new \RuntimeException('Accepted WebSocket handshake did not include a request.'),
            );
            return;
        }

        try {
            $accepted = $this->connections->create(
                socket: $connection->socket,
                id: bin2hex(random_bytes(8)),
                request: $response->request,
            );
        } catch (\Throwable $e) {
            $this->removePending(
                $id,
                $pending,
                $pendingWatchers,
                $pendingWriteWatchers,
                $pendingResponses,
                closeSocket: true,
            );
            $this->fail($application, WebSocketErrorPhase::HANDSHAKE, null, $e);
            return;
        }

        $this->removePending(
            $id,
            $pending,
            $pendingWatchers,
            $pendingWriteWatchers,
            $pendingResponses,
            closeSocket: false,
        );
        $this->lastActivityAt[$accepted->socket->id] = $this->nowMs();
        unset($this->awaitingPongSince[$accepted->socket->id]);
        $connections[$accepted->socket->id] = $accepted;
        $readWatchers[$accepted->socket->id] = $this->loop->onReadable(
            $accepted->socket,
            function () use ($accepted, &$connections, &$readWatchers, &$writeWatchers, $application): void {
                $this->processConnection(
                    $accepted->socket->id,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                );
            },
        );
        $this->applications->connected($application, $accepted);
        $this->flushOrWatchConnection($accepted, $connections, $readWatchers, $writeWatchers, $application);
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function processConnection(
        int|string $id,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        $connection = $connections[$id] ?? null;

        if ($connection === null) {
            return;
        }

        try {
            $read = $connection->socket->read(8192);
        } catch (\Throwable $e) {
            $this->fail($application, WebSocketErrorPhase::READ, $connection, $e);
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                CloseInfo::transport('Unable to read from socket.'),
            );
            return;
        }

        if ($read->status === SocketReadStatus::ERROR) {
            $this->fail(
                $application,
                WebSocketErrorPhase::READ,
                $connection,
                new \RuntimeException($read->reason ?? 'Unable to read from socket.'),
            );
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                $connection->closeInfo ?? CloseInfo::transport($read->reason ?? 'Socket read failed.'),
            );
            return;
        }

        if ($read->status === SocketReadStatus::CLOSED) {
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                $connection->closeInfo ?? CloseInfo::transport($read->reason ?? 'Socket closed.'),
            );
            return;
        }

        if ($read->status === SocketReadStatus::EMPTY) {
            return;
        }

        $pongCount = $connection->receivedPongs;
        $this->lastActivityAt[$connection->socket->id] = $this->nowMs();

        try {
            $messages = $connection->appendBytes($read->bytes);
        } catch (ProtocolException $e) {
            $this->closeAfterProtocolError($connection, $connections, $readWatchers, $writeWatchers, $application, $e);
            return;
        } catch (\Throwable $e) {
            $result = $connection->close(CloseCode::INTERNAL_ERROR, 'Internal server error');
            $this->fail($application, WebSocketErrorPhase::READ, $connection, $e);

            if (!$result->accepted && !$connection->needsWrite) {
                $this->closeConnection(
                    $connection,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                    CloseInfo::transport('Unable to queue internal error close frame.'),
                );
                return;
            }

            $this->flushOrWatchConnection($connection, $connections, $readWatchers, $writeWatchers, $application);
            return;
        }

        if ($connection->receivedPongs > $pongCount) {
            unset($this->awaitingPongSince[$connection->socket->id]);
        }

        foreach ($messages as $message) {
            $this->applications->received($application, $connection, $message);

            if (!$this->running) {
                break;
            }
        }

        $this->flushOrWatchConnection($connection, $connections, $readWatchers, $writeWatchers, $application);
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function flushOrWatchConnection(
        TransportConnectionInterface $connection,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        if (!$connection->needsWrite) {
            return;
        }

        $id = $connection->socket->id;
        $this->flushConnection($id, $connections, $readWatchers, $writeWatchers, $application);

        if (isset($connections[$id]) && $connection->needsWrite) {
            $this->ensureWriteWatcher($connection, $connections, $readWatchers, $writeWatchers, $application);
        }
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function closeAfterProtocolError(
        TransportConnectionInterface $connection,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
        ProtocolException $e,
    ): void {
        $result = $connection->close($e->closeCode, $e->getMessage());
        $this->fail($application, WebSocketErrorPhase::READ, $connection, $e);

        if (!$result->accepted && !$connection->needsWrite) {
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                CloseInfo::transport('Unable to queue protocol error close frame.'),
            );
            return;
        }

        $this->flushOrWatchConnection($connection, $connections, $readWatchers, $writeWatchers, $application);
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function requestConnectionClose(
        TransportConnectionInterface $connection,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
        int $code,
        string $reason,
        string $fallbackReason,
    ): void {
        $result = $connection->close($code, $reason);

        if (!$result->accepted && !$connection->closing) {
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                CloseInfo::transport($fallbackReason),
            );
            return;
        }

        $this->flushOrWatchConnection($connection, $connections, $readWatchers, $writeWatchers, $application);
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function ensureWriteWatcher(
        TransportConnectionInterface $connection,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        if (!$connection->needsWrite || isset($writeWatchers[$connection->socket->id])) {
            return;
        }

        $writeWatchers[$connection->socket->id] = $this->loop->onWritable(
            $connection->socket,
            function () use ($connection, &$connections, &$readWatchers, &$writeWatchers, $application): void {
                $this->flushConnection(
                    $connection->socket->id,
                    $connections,
                    $readWatchers,
                    $writeWatchers,
                    $application,
                );
            },
        );
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function flushConnection(
        int|string $id,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        $connection = $connections[$id] ?? null;

        if ($connection === null) {
            ($writeWatchers[$id] ?? null)?->cancel();
            unset($writeWatchers[$id]);
            return;
        }

        try {
            $flushed = $connection->flush();
        } catch (\Throwable $e) {
            $this->fail($application, WebSocketErrorPhase::WRITE, $connection, $e);
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                CloseInfo::transport('Unable to write to socket.'),
            );
            return;
        }

        if (!$flushed) {
            $this->fail(
                $application,
                WebSocketErrorPhase::WRITE,
                $connection,
                new \RuntimeException('Unable to write to socket.'),
            );
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                CloseInfo::transport('Unable to write to socket.'),
            );
            return;
        }

        if (!$connection->needsWrite) {
            ($writeWatchers[$id] ?? null)?->cancel();
            unset($writeWatchers[$id]);
        }

        if ($connection->closing && !$connection->needsWrite) {
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                $connection->closeInfo ?? CloseInfo::local(),
            );
        }
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function drainConnectionsForShutdown(
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
    ): void {
        foreach ($connections as $connection) {
            if (!$connection->closing && !$connection->closed) {
                $connection->close(CloseCode::GOING_AWAY, 'Server shutdown');
            }
        }

        $attempts = $this->options->shutdownDrainTimeoutMs === 0
            ? 0
            : min(64, max(1, $this->options->shutdownDrainTimeoutMs));

        for ($attempt = 0; $attempt < $attempts && $connections !== []; $attempt++) {
            $progress = false;

            foreach ($connections as $connection) {
                $queuedBefore = $connection->queuedBytes;

                if ($queuedBefore > 0 && !$connection->flush()) {
                    $this->closeConnection(
                        $connection,
                        $connections,
                        $readWatchers,
                        $writeWatchers,
                        $application,
                        CloseInfo::transport('Unable to write shutdown close frame.'),
                    );
                    continue;
                }

                if ($connection->queuedBytes < $queuedBefore) {
                    $progress = true;
                }

                if ($connection->closing && !$connection->needsWrite) {
                    $this->closeConnection(
                        $connection,
                        $connections,
                        $readWatchers,
                        $writeWatchers,
                        $application,
                        $connection->closeInfo ?? CloseInfo::serverShutdown(),
                    );
                }
            }

            if (!$progress) {
                break;
            }
        }

        foreach ($connections as $connection) {
            $this->closeConnection(
                $connection,
                $connections,
                $readWatchers,
                $writeWatchers,
                $application,
                $connection->closeInfo ?? CloseInfo::serverShutdown(),
            );
        }
    }

    /**
     * @param array<int|string, TransportConnectionInterface> $connections
     * @param array<int|string, LoopWatcherInterface> $readWatchers
     * @param array<int|string, LoopWatcherInterface> $writeWatchers
     */
    private function closeConnection(
        TransportConnectionInterface $connection,
        array &$connections,
        array &$readWatchers,
        array &$writeWatchers,
        WebSocketApplicationInterface $application,
        CloseInfo $close,
    ): void {
        if ($connection->closed) {
            return;
        }

        $id = $connection->socket->id;
        ($readWatchers[$id] ?? null)?->cancel();
        ($writeWatchers[$id] ?? null)?->cancel();
        unset($readWatchers[$id], $writeWatchers[$id], $connections[$id]);
        unset($this->lastActivityAt[$id], $this->awaitingPongSince[$id]);

        $connection->markClosed($close);
        $connection->socket->close();
        $this->applications->disconnected($application, $connection, $connection->closeInfo ?? $close);
    }

    /**
     * @param array<int|string, PendingConnectionInterface> $pending
     * @param array<int|string, LoopWatcherInterface> $pendingWatchers
     * @param array<int|string, LoopWatcherInterface> $pendingWriteWatchers
     * @param array<int|string, HandshakeResponse> $pendingResponses
     */
    private function removePending(
        int|string $id,
        array &$pending,
        array &$pendingWatchers,
        array &$pendingWriteWatchers,
        array &$pendingResponses,
        bool $closeSocket,
    ): void {
        $connection = $pending[$id] ?? null;
        ($pendingWatchers[$id] ?? null)?->cancel();
        ($pendingWriteWatchers[$id] ?? null)?->cancel();
        unset($pendingWatchers[$id], $pendingWriteWatchers[$id], $pendingResponses[$id], $pending[$id]);

        if ($closeSocket && $connection !== null) {
            $connection->socket->close();
        }
    }

    /**
     * @param array<int|string, LoopWatcherInterface> $watchers
     */
    private function cancelWatchers(array &$watchers): void
    {
        foreach ($watchers as $watcher) {
            $watcher->cancel();
        }

        $watchers = [];
    }

    private function fail(
        WebSocketApplicationInterface $application,
        WebSocketErrorPhase $phase,
        ?ConnectionInterface $connection,
        \Throwable $error,
    ): void {
        $this->applications->failed($application, $this->errors->create($phase, $connection, $error));
    }

    private function writeImmediate(SocketInterface $socket, string $data): bool
    {
        while ($data !== '') {
            $written = $socket->write($data);

            if ($written->status !== SocketWriteStatus::WRITTEN || $written->writtenBytes === 0) {
                return false;
            }

            $data = substr($data, $written->writtenBytes);
        }

        return true;
    }

    private function httpClose(int $status, string $reason): string
    {
        $body = $reason . "\n";

        return sprintf(
            "HTTP/1.1 %d %s\r\nConnection: close\r\nContent-Length: %d\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s",
            $status,
            $reason,
            strlen($body),
            $body,
        );
    }

    private function nowMs(): float
    {
        return (float) $this->clock->now()->format('U.u') * 1000;
    }
}
