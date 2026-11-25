<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

/** Monotonic elapsed-time source; values are meaningful only as differences. */
interface MonotonicClockInterface
{
    public function nowMilliseconds(): float;
}
