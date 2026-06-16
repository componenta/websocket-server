<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

interface WorkerInterface
{
    public WorkerDescriptorInterface $descriptor { get; }

    public function start(): void;

    public function drain(): void;

    public function stop(): void;
}
