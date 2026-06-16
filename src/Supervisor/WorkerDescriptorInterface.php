<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerDescriptorInterface extends Arrayable
{
    public string $id { get; }

    public WorkerAddressInterface $address { get; }

    public WorkerCapacityInterface $capacity { get; }

    public WorkerState $state { get; }

    public ?int $pid { get; }

    public ?\DateTimeImmutable $startedAt { get; }

    /**
     * @return array{
     *     id: string,
     *     address: array{host: string, port: int},
     *     capacity: array{connections: int, maxConnections: int, acceptingConnections: bool},
     *     state: string,
     *     pid: int|null,
     *     startedAt: string|null
     * }
     */
    public function toArray(): array;
}
