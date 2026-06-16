<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerMessageTargetInterface extends Arrayable
{
    public ?string $workerId { get; }

    public ?string $connectionId { get; }

    public ?string $userId { get; }

    public ?string $channel { get; }

    /**
     * @return array{workerId: string|null, connectionId: string|null, userId: string|null, channel: string|null}
     */
    public function toArray(): array;
}
