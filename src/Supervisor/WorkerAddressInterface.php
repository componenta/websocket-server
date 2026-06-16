<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerAddressInterface extends Arrayable
{
    public string $host { get; }

    public int $port { get; }

    /**
     * @return array{host: string, port: int}
     */
    public function toArray(): array;
}
