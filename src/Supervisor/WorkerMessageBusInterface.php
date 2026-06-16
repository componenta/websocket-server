<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

interface WorkerMessageBusInterface
{
    public function publish(WorkerMessageInterface $message): void;

    /**
     * @return iterable<WorkerMessageInterface>
     */
    public function consume(string $workerId): iterable;
}
