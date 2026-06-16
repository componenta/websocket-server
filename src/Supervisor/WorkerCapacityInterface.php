<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

use Componenta\Arrayable\Arrayable;

interface WorkerCapacityInterface extends Arrayable
{
    public int $connections { get; }

    public int $maxConnections { get; }

    public bool $acceptingConnections { get; }

    /**
     * @return array{connections: int, maxConnections: int, acceptingConnections: bool}
     */
    public function toArray(): array;
}
