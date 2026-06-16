<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\Arrayable\Arrayable;

/**
 * @extends Arrayable<string, ConnectionInterface>
 */
interface ConnectionRegistryInterface extends \Countable, Arrayable
{
    public function add(ConnectionInterface $connection): void;

    public function remove(ConnectionInterface|string $connection): void;

    public function get(string $id): ?ConnectionInterface;

    /**
     * @return array<string, ConnectionInterface>
     */
    public function toArray(): array;
}
