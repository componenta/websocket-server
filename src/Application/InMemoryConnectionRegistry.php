<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Connection\ConnectionInterface;

final class InMemoryConnectionRegistry implements ConnectionRegistryInterface
{
    /** @var array<string, ConnectionInterface> */
    private array $connections = [];

    public function add(ConnectionInterface $connection): void
    {
        $this->connections[$connection->id] = $connection;
    }

    public function remove(ConnectionInterface|string $connection): void
    {
        unset($this->connections[$connection instanceof ConnectionInterface ? $connection->id : $connection]);
    }

    public function get(string $id): ?ConnectionInterface
    {
        return $this->connections[$id] ?? null;
    }

    public function toArray(): array
    {
        return $this->connections;
    }

    public function count(): int
    {
        return count($this->connections);
    }
}
