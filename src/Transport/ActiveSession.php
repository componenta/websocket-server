<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Connection\TransportConnectionInterface;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

/** @internal Owns connection-specific runtime registrations and heartbeat state. */
final class ActiveSession
{
    public readonly SessionWatchers $watchers;
    public private(set) bool $announced = false;
    public private(set) bool $processingInput = false;
    private ?float $queuedPingAt = null;
    private ?float $sentPingAt = null;
    private bool $shutdownInitiated = false;

    public function __construct(
        public readonly TransportConnectionInterface $connection,
        public private(set) float $lastActivityAt,
    ) {
        $this->watchers = new SessionWatchers();
    }

    public function beginShutdown(): bool
    {
        if ($this->connection->closing || $this->connection->closed) {
            return true;
        }

        $this->shutdownInitiated = $this->connection->close(CloseCode::GOING_AWAY, 'Server shutdown')->accepted;
        return $this->shutdownInitiated;
    }

    public function shutdownCloseInfo(): CloseInfo
    {
        return $this->shutdownInitiated
            ? CloseInfo::serverShutdown()
            : ($this->connection->closeInfo ?? CloseInfo::serverShutdown());
    }

    public function announce(): void
    {
        $this->announced = true;
    }

    public function recordActivity(float $now): void
    {
        $this->lastActivityAt = $now;
    }

    /** @param \Closure(Message): void $onMessage */
    public function receiveBytes(string $bytes, \Closure $onMessage): void
    {
        $this->processingInput = true;
        try {
            $this->connection->receiveBytes($bytes, $onMessage);
        } finally {
            $this->processingInput = false;
        }
    }

    public function pingQueued(float $now): void
    {
        $this->queuedPingAt = $now;
    }

    public function afterFlush(float $now): void
    {
        if ($this->connection->pingSent && $this->queuedPingAt !== null) {
            $this->queuedPingAt = null;
            $this->sentPingAt = $now;
        }
    }

    public function clearHeartbeat(): void
    {
        $this->queuedPingAt = $this->sentPingAt = null;
    }

    public function heartbeatFailure(float $now, int $timeoutMs): ?CloseInfo
    {
        if ($this->queuedPingAt !== null && $now - $this->queuedPingAt >= $timeoutMs) {
            return CloseInfo::transport('WebSocket ping write timeout.');
        }

        if ($this->sentPingAt !== null && $now - $this->sentPingAt >= $timeoutMs) {
            return CloseInfo::transport('WebSocket pong timeout.');
        }

        return null;
    }
}
