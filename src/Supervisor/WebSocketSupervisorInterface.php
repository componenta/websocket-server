<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

interface WebSocketSupervisorInterface
{
    public function start(): void;

    public function drain(?string $workerId = null): void;

    public function stop(?string $workerId = null): void;
}
