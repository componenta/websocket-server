<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Connection\TransportConnectionInterface;
use Componenta\WebSocket\Protocol\CloseInfo;

/** @internal Owns connection-specific runtime registrations and heartbeat state. */
final class ActiveSession
{
    public readonly SessionWatchers $watchers;
    public private(set) bool $announced = false;
    private ?float $queuedPingAt = null;
    private ?float $sentPingAt = null;

    public function __construct(
        public readonly TransportConnectionInterface $connection,
        public private(set) float $lastActivityAt,
    ) {
        $this->watchers = new SessionWatchers();
    }

    public function announce(): void
    {
        $this->announced = true;
    }

    public function recordActivity(float $now): void
    {
        $this->lastActivityAt = $now;
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
