<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

final readonly class MonotonicClock implements MonotonicClockInterface
{
    public function nowMilliseconds(): float
    {
        return hrtime(true) / 1_000_000;
    }
}
