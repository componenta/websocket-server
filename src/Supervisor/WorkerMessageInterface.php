<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerMessageInterface extends Arrayable
{
    public string $id { get; }

    public string $type { get; }

    public WorkerMessageTargetInterface $target { get; }

    public array $payload { get; }

    public \DateTimeImmutable $createdAt { get; }

    /**
     * @return array{
     *     id: string,
     *     type: string,
     *     target: array{workerId: string|null, connectionId: string|null, userId: string|null, channel: string|null},
     *     payload: array<string, mixed>,
     *     createdAt: string
     * }
     */
    public function toArray(): array;
}
