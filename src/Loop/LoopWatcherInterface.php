<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

interface LoopWatcherInterface
{
    public int|string $id { get; }

    public bool $enabled { get; }

    public function cancel(): void;
}
