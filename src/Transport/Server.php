<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\WebSocketApplicationInvokerInterface;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\ConnectionFactoryInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactoryInterface;
use Componenta\WebSocket\Connection\TransportConnectionInterface;
use Componenta\WebSocket\Exception\ProtocolException;
use Componenta\WebSocket\Loop\EventLoopInterface;
use Componenta\WebSocket\Loop\MonotonicClockInterface;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
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
    private bool $executing = false;

    public function __construct(
        private readonly WebSocketOptionsInterface $options,
        private readonly SocketListenerFactoryInterface $listeners,
        private readonly EventLoopInterface $loop,
        private readonly HandshakeInterface $handshake,
        private readonly ConnectionFactoryInterface $connections,
        private readonly PendingConnectionFactoryInterface $pendingConnections,
        private readonly WebSocketApplicationInvokerInterface $applications,
        private readonly WebSocketErrorContextFactoryInterface $errors,
        private readonly MonotonicClockInterface $clock,
    ) {}

    public function run(WebSocketApplicationInterface $application): void
    {
        if ($this->executing) {
            throw new \LogicException('WebSocket server is already running.');
        }

        $this->executing = true;
        try {
            $state = new ServerRunState(
                $application,
                $this->options->maxTotalOutgoingBufferSize,
                $this->options->maxTotalIncomingBufferSize,
            );
            $listener = $this->listeners->listen($this->options);
            $this->running = true;

            try {
                $state->watchers->watchRead($this->loop, $listener, fn () => $this->accept($state, $listener));
                $this->scheduleLifecycle($state);
                $this->loop->run();
            } catch (\Throwable $error) {
                $this->fail($state, WebSocketErrorPhase::SELECT, null, $error);
            } finally {
                $this->running = false;
                $this->shutdown($state, $listener);
            }
        } finally {
            $this->executing = false;
        }
    }

    public function stop(): void
    {
        if (!$this->running) {
            return;
        }

        $this->running = false;
        $this->loop->stop();
    }

    private function accept(ServerRunState $state, SocketListenerInterface $listener): void
    {
        for ($i = 0; $i < 64 && $this->running; $i++) {
            try {
                $socket = $listener->accept();
            } catch (\Throwable $error) {
                $this->fail($state, WebSocketErrorPhase::ACCEPT, null, $error);
                return;
            }

            if ($socket === null) {
                return;
            }

            if ($state->pendingCount() >= $this->options->maxPendingConnections
                || $state->pendingCount() + $state->activeCount() >= $this->options->maxConnections
            ) {
                try {
                    $this->writeImmediate($socket, $this->httpClose(503, 'Service Unavailable'));
                } catch (\Throwable $error) {
                    $this->fail($state, WebSocketErrorPhase::ACCEPT, null, $error);
                } finally {
                    $this->closeSocket($state, $socket);
                }
                continue;
            }

            $session = null;
            try {
                $session = new PendingSession($this->pendingConnections->create($socket));
                $state->addPending($session);
                $session->watchers->watchRead($this->loop, $socket, fn () => $this->readHandshake($state, $session));
                $session->watchers->schedule($this->loop, $this->options->pendingHandshakeTimeoutMs,
                    fn () => $this->discardPending($state, $session));
            } catch (\Throwable $error) {
                if ($session !== null && $state->hasPending($session)) {
                    $this->discardPending($state, $session);
                } else {
                    $this->closeSocket($state, $socket);
                }
                $this->fail($state, WebSocketErrorPhase::ACCEPT, null, $error);
            }
        }
    }

    private function readHandshake(ServerRunState $state, PendingSession $session): void
    {
        if (!$state->hasPending($session)) {
            return;
        }

        $connection = $session->connection;
        try {
            if ($connection->expired($this->clock->nowMilliseconds(), $this->options->pendingHandshakeTimeoutMs)) {
                $this->discardPending($state, $session);
                return;
            }

            $read = $connection->socket->read(8192);
            if ($read->status === SocketReadStatus::ERROR || $read->status === SocketReadStatus::CLOSED) {
                $this->discardPending($state, $session);
                return;
            }
            if ($read->status === SocketReadStatus::EMPTY) {
                return;
            }

            $connection->append($read->bytes);
            if ($connection->tooLarge($this->options->maxPendingHandshakeBytes)) {
                $response = new HandshakeResponse(false, 431, $this->httpClose(431, 'Request Header Fields Too Large'));
            } elseif ($connection->ready) {
                $response = $this->handshake->accept($connection->buffer);
            } else {
                return;
            }

            $session->respond($response);
            $this->flushHandshake($state, $session);
        } catch (\Throwable $error) {
            $this->discardPending($state, $session);
            $this->fail($state, WebSocketErrorPhase::HANDSHAKE, null, $error);
        }
    }

    private function flushHandshake(ServerRunState $state, PendingSession $session): void
    {
        if (!$state->hasPending($session)) {
            return;
        }

        $connection = $session->connection;
        try {
            if ($connection->expired($this->clock->nowMilliseconds(), $this->options->pendingHandshakeTimeoutMs)) {
                $this->discardPending($state, $session);
                return;
            }

            if (!$connection->flush()) {
                throw new \RuntimeException('Unable to write WebSocket handshake response.');
            }

            if ($connection->needsWrite) {
                $session->watchers->watchWrite($this->loop, $connection->socket,
                    fn () => $this->flushHandshake($state, $session));
                return;
            }
        } catch (\Throwable $error) {
            $this->discardPending($state, $session);
            $this->fail($state, WebSocketErrorPhase::WRITE, null, $error);
            return;
        }

        $response = $session->response;
        if ($response === null || ($response->accepted && $response->request === null)) {
            $this->discardPending($state, $session);
            $this->fail($state, WebSocketErrorPhase::HANDSHAKE, null,
                new \RuntimeException('Accepted WebSocket handshake did not include a request.'));
            return;
        }
        if (!$response->accepted) {
            $this->discardPending($state, $session);
            return;
        }

        $this->activate($state, $session, $response);
    }

    private function activate(ServerRunState $state, PendingSession $pending, HandshakeResponse $response): void
    {
        $session = null;
        try {
            $connection = $this->connections->create(
                socket: $pending->connection->socket,
                id: bin2hex(random_bytes(16)),
                request: $response->request ?? throw new \LogicException('Accepted handshake requires a request.'),
                outgoingBudget: $state->outgoingBudget,
                incomingBudget: $state->incomingBudget,
            );
            $session = new ActiveSession($connection, $this->clock->nowMilliseconds());
            $this->cancelWatchers($state, $pending->watchers);
            $state->activate($pending, $session);

            $session->watchers->watchRead($this->loop, $connection->socket,
                fn () => $this->readConnection($state, $session));
            $connection->setWriteNotifier(fn () => $this->scheduleWrite($state, $session));
        } catch (\Throwable $error) {
            if ($session !== null && $state->hasActive($session)) {
                $this->closeConnection($state, $session, CloseInfo::transport('Connection registration failed.'));
            } else {
                $this->discardPending($state, $pending);
            }
            $this->fail($state, WebSocketErrorPhase::HANDSHAKE, null, $error);
            return;
        }

        if (!$state->hasActive($session)) {
            return;
        }
        $session->announce();
        if (!$this->applications->connected($state->application, $connection->application) && !$connection->closing) {
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to queue application error close frame.'));
            return;
        }
        $this->flushOrWatch($state, $session);
    }

    private function readConnection(ServerRunState $state, ActiveSession $session): void
    {
        if (!$state->hasActive($session)) {
            return;
        }

        $connection = $session->connection;
        try {
            $read = $connection->socket->read(8192);
        } catch (\Throwable $error) {
            $this->fail($state, WebSocketErrorPhase::READ, $connection, $error);
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to read from socket.'));
            return;
        }

        if ($read->status === SocketReadStatus::ERROR) {
            $reason = $read->reason ?? 'Unable to read from socket.';
            $this->fail($state, WebSocketErrorPhase::READ, $connection, new \RuntimeException($reason));
            $this->closeConnection($state, $session, CloseInfo::transport($reason));
            return;
        }
        if ($read->status === SocketReadStatus::CLOSED) {
            $this->closeConnection($state, $session, CloseInfo::transport($read->reason ?? 'Socket closed.'));
            return;
        }
        if ($read->status === SocketReadStatus::EMPTY) {
            return;
        }

        $session->recordActivity($this->clock->nowMilliseconds());
        try {
            $session->receiveBytes($read->bytes, function (Message $message) use ($state, $session): void {
                $this->deliverMessage($state, $session, $message);
            });
        } catch (ProtocolException $error) {
            $this->protocolFailure($state, $session, $error);
            return;
        } catch (\Throwable $error) {
            $this->fail($state, WebSocketErrorPhase::READ, $connection, $error);
            $this->requestClose($state, $session, CloseCode::INTERNAL_ERROR, 'Internal server error',
                'Unable to queue internal error close frame.');
            return;
        }

        if (!$connection->awaitingPong) {
            $session->clearHeartbeat();
        }

        $this->flushOrWatch($state, $session);
    }

    private function deliverMessage(ServerRunState $state, ActiveSession $session, Message $message): void
    {
        $connection = $session->connection;
        if (!$this->running || !$state->hasActive($session) || $connection->closing || $connection->closed) {
            return;
        }

        $completed = $this->applications->received($state->application, $connection->application, $message);
        if (!$completed && !$connection->closing) {
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to queue application error close frame.'));
        }
    }

    private function protocolFailure(ServerRunState $state, ActiveSession $session, ProtocolException $error): void
    {
        $connection = $session->connection;

        try {
            $result = $connection->failProtocol($error->closeCode, 'WebSocket protocol error');
        } catch (\Throwable $failure) {
            $this->fail($state, WebSocketErrorPhase::READ, $connection, $error);
            $this->fail($state, WebSocketErrorPhase::WRITE, $connection, $failure);
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to queue protocol error close frame.'));
            return;
        }

        $this->fail($state, WebSocketErrorPhase::READ, $connection, $error);

        if (!$result->accepted) {
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to queue protocol error close frame.'));
            return;
        }
        $this->flushOrWatch($state, $session);
    }

    private function requestClose(ServerRunState $state, ActiveSession $session, int $code, string $reason, string $failure): void
    {
        $connection = $session->connection;
        try {
            $result = $connection->close($code, $reason);
        } catch (\Throwable $error) {
            $this->fail($state, WebSocketErrorPhase::WRITE, $connection, $error);
            $this->closeConnection($state, $session, CloseInfo::transport($failure));
            return;
        }

        if (!$result->accepted && !$connection->closing) {
            $this->closeConnection($state, $session, CloseInfo::transport($failure));
            return;
        }
        $this->flushOrWatch($state, $session);
    }

    private function scheduleWrite(ServerRunState $state, ActiveSession $session): void
    {
        if (!$this->running || !$state->hasActive($session)) {
            return;
        }

        $connection = $session->connection;
        try {
            if ($connection->closing) {
                $session->clearHeartbeat();
                $session->watchers->schedule($this->loop, $this->options->closeHandshakeTimeoutMs,
                    fn () => $this->closeConnection($state, $session,
                        $connection->closeInfo ?? CloseInfo::transport('WebSocket close handshake timed out.')));
            }

            // The current read callback will attempt an immediate flush before
            // returning to the loop. Register only if data remains queued then.
            // Other sessions still register their own writers synchronously.
            if ($connection->needsWrite && !$session->processingInput) {
                $session->watchers->watchWrite($this->loop, $connection->socket,
                    fn () => $this->flushConnection($state, $session));
            }
        } catch (\Throwable $error) {
            // The write belongs to the target session, not the application
            // callback (possibly running for a different connection).
            $this->closeConnection(
                $state,
                $session,
                CloseInfo::transport('Unable to register WebSocket write.'),
                $error,
            );
        }
    }

    private function flushOrWatch(ServerRunState $state, ActiveSession $session): void
    {
        if (!$state->hasActive($session)) {
            return;
        }

        if ($session->connection->readyToClose) {
            $this->closeConnection($state, $session, $session->connection->closeInfo ?? CloseInfo::local());
            return;
        }
        if ($session->connection->needsWrite) {
            $this->flushConnection($state, $session);
        }
        $this->scheduleWrite($state, $session);
    }

    private function flushConnection(ServerRunState $state, ActiveSession $session): void
    {
        if (!$state->hasActive($session)) {
            return;
        }

        $connection = $session->connection;
        try {
            if (!$connection->flush()) {
                throw new \RuntimeException('Unable to write to socket.');
            }
            $session->afterFlush($this->clock->nowMilliseconds());
            if (!$connection->needsWrite) {
                $session->watchers->cancelWrite();
            }
        } catch (\Throwable $error) {
            $this->fail($state, WebSocketErrorPhase::WRITE, $connection, $error);
            $this->closeConnection($state, $session, CloseInfo::transport('Unable to write to socket.'));
            return;
        }

        if ($connection->readyToClose) {
            $this->closeConnection($state, $session, $connection->closeInfo ?? CloseInfo::local());
        }
    }

    private function scheduleLifecycle(ServerRunState $state): void
    {
        if (!$this->running) {
            return;
        }
        $intervals = [];
        if ($this->options->heartbeatIntervalMs > 0) {
            $intervals[] = $this->options->heartbeatIntervalMs;
            $intervals[] = $this->options->pongTimeoutMs;
        }
        if ($this->options->idleTimeoutMs > 0) {
            $intervals[] = $this->options->idleTimeoutMs;
        }
        if ($intervals === []) {
            return;
        }

        $state->watchers->schedule($this->loop, min($intervals), function () use ($state): void {
            $this->checkLifecycle($state);
            $this->scheduleLifecycle($state);
        });
    }

    private function checkLifecycle(ServerRunState $state): void
    {
        $now = $this->clock->nowMilliseconds();

        foreach ($state->active() as $session) {
            if (!$this->running) {
                break;
            }

            $connection = $session->connection;
            if ($connection->closed || $connection->closing) {
                continue;
            }
            $failure = $session->heartbeatFailure($now, $this->options->pongTimeoutMs);
            if ($failure !== null) {
                $this->closeConnection($state, $session, $failure);
                continue;
            }

            $idle = $now - $session->lastActivityAt;
            if ($this->options->idleTimeoutMs > 0 && $idle >= $this->options->idleTimeoutMs) {
                $this->requestClose($state, $session, CloseCode::NORMAL, 'Idle timeout',
                    'Unable to queue idle timeout close frame.');
                continue;
            }
            if ($this->options->heartbeatIntervalMs < 1 || $idle < $this->options->heartbeatIntervalMs
                || $connection->awaitingPong
            ) {
                continue;
            }

            try {
                if (!$connection->ping(random_bytes(8))->accepted) {
                    $this->closeConnection($state, $session, CloseInfo::transport('Unable to queue WebSocket ping frame.'));
                    continue;
                }
                $session->pingQueued($now);
                $this->flushOrWatch($state, $session);
            } catch (\Throwable $error) {
                $this->fail($state, WebSocketErrorPhase::WRITE, $connection, $error);
                $this->closeConnection($state, $session, CloseInfo::transport('Unable to send WebSocket ping.'));
            }
        }
    }

    private function discardPending(ServerRunState $state, PendingSession $session): void
    {
        if (!$state->removePending($session)) {
            return;
        }
        $this->cancelWatchers($state, $session->watchers);
        $this->closeSocket($state, $session->connection->socket);
    }

    private function closeConnection(
        ServerRunState $state,
        ActiveSession $session,
        CloseInfo $close,
        ?\Throwable $writeRegistrationFailure = null,
    ): void {
        if (!$state->removeActive($session)) {
            return;
        }
        $connection = $session->connection;
        $this->cancelWatchers($state, $session->watchers, $connection);

        try {
            $connection->setWriteNotifier(null);
        } catch (\Throwable $error) {
            $this->cleanupFailure($state, $connection, $error);
        }
        try {
            $connection->markClosed($close);
        } catch (\Throwable $error) {
            $this->cleanupFailure($state, $connection, $error);
        }

        $this->closeSocket($state, $connection->socket, $connection);

        if ($writeRegistrationFailure !== null) {
            try {
                $this->fail($state, WebSocketErrorPhase::WRITE, $connection, $writeRegistrationFailure);
            } catch (\Throwable $error) {
                $this->cleanupFailure($state, $connection, $error);
            }
        }

        if ($session->announced) {
            try {
                $this->applications->disconnected($state->application, $connection->application, $connection->closeInfo ?? $close);
            } catch (\Throwable $error) {
                $this->cleanupFailure($state, $connection, $error);
            }
        }
    }

    private function shutdown(ServerRunState $state, SocketListenerInterface $listener): void
    {
        $this->cancelWatchers($state, $state->watchers);
        // Release the listening endpoint before draining clients. Otherwise
        // the kernel can still accept TCP handshakes into the unused backlog.
        $this->closeSocket($state, $listener);
        foreach ($state->pending() as $session) {
            $this->discardPending($state, $session);
        }
        foreach ($state->active() as $session) {
            $this->cancelWatchers($state, $session->watchers, $session->connection);
        }

        try {
            (new ShutdownCoordinator($this->clock))->drain(
                $state, $this->options->shutdownDrainTimeoutMs,
                fn (ActiveSession $session, CloseInfo $close) => $this->closeConnection($state, $session, $close),
                fn (ActiveSession $session, \Throwable $error) => $this->cleanupFailure($state, $session->connection, $error),
            );
        } catch (\Throwable $error) {
            $this->cleanupFailure($state, null, $error);
        } finally {
            foreach ($state->active() as $session) {
                $this->closeConnection($state, $session, $session->shutdownCloseInfo());
            }
        }
    }

    private function cancelWatchers(ServerRunState $state, SessionWatchers $watchers, ?ConnectionInterface $connection = null): void
    {
        foreach ($watchers->cancelAll() as $error) {
            $this->cleanupFailure($state, $connection, $error);
        }
    }

    private function closeSocket(ServerRunState $state, SocketInterface|SocketListenerInterface $socket, ?ConnectionInterface $connection = null): void
    {
        try {
            $socket->close();
        } catch (\Throwable $error) {
            $this->cleanupFailure($state, $connection, $error);
        }
    }

    private function fail(ServerRunState $state, WebSocketErrorPhase $phase, ?ConnectionInterface $connection, \Throwable $error): void
    {
        $handle = $connection instanceof TransportConnectionInterface ? $connection->application : $connection;
        $this->applications->failed($state->application, $this->errors->create($phase, $handle, $error));
    }

    private function cleanupFailure(ServerRunState $state, ?ConnectionInterface $connection, \Throwable $error): void
    {
        try {
            $this->fail($state, WebSocketErrorPhase::SHUTDOWN, $connection, $error);
        } catch (\Throwable) {
            // The error reporter must not prevent releasing other resources.
        }
    }

    private function writeImmediate(SocketInterface $socket, string $data): void
    {
        while ($data !== '') {
            $written = $socket->write($data);
            if ($written->status !== SocketWriteStatus::WRITTEN || $written->writtenBytes <= 0) {
                return;
            }
            $data = substr($data, $written->writtenBytes);
        }
    }

    private function httpClose(int $status, string $reason): string
    {
        $body = $reason . "\n";
        return sprintf(
            "HTTP/1.1 %d %s\r\nConnection: close\r\nContent-Length: %d\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s",
            $status, $reason, strlen($body), $body,
        );
    }
}
