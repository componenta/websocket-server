<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

interface LoopTimerInterface
{
    public int|string $id { get; }

    public bool $enabled { get; }

    public function cancel(): void;
}
