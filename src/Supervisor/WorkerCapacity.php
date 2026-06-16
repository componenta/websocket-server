<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

final class WorkerCapacity implements WorkerCapacityInterface
{
    public bool $acceptingConnections {
        get => $this->connections < $this->maxConnections;
    }

    public function __construct(
        public readonly int $connections,
        public readonly int $maxConnections,
    ) {
        if ($connections < 0) {
            throw new \InvalidArgumentException('WebSocket worker connection count must not be negative.');
        }

        if ($maxConnections < 1) {
            throw new \InvalidArgumentException('WebSocket worker max connections must be positive.');
        }

        if ($connections > $maxConnections) {
            throw new \InvalidArgumentException('WebSocket worker connections must not exceed max connections.');
        }
    }

    public function toArray(): array
    {
        return [
            'connections' => $this->connections,
            'maxConnections' => $this->maxConnections,
            'acceptingConnections' => $this->acceptingConnections,
        ];
    }
}
