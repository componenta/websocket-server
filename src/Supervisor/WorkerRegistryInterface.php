<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerRegistryInterface extends \Countable, Arrayable
{
    /**
     * @var iterable<string, WorkerDescriptorInterface>
     */
    public iterable $workers { get; }

    public function register(WorkerDescriptorInterface $worker): void;

    public function unregister(string $workerId): void;

    public function get(string $workerId): ?WorkerDescriptorInterface;

    /**
     * @return array<string, array{
     *     id: string,
     *     address: array{host: string, port: int},
     *     capacity: array{connections: int, maxConnections: int, acceptingConnections: bool},
     *     state: string,
     *     pid: int|null,
     *     startedAt: string|null
     * }>
     */
    public function toArray(): array;
}
