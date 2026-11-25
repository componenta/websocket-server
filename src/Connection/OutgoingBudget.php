<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

/** @internal Shared limit for queued wire bytes within one server run. */
final class OutgoingBudget
{
    private int $used = 0;

    public function __construct(private readonly int $limit)
    {
        if ($limit < 1) {
            throw new \InvalidArgumentException('Outgoing queue budget must be positive.');
        }
    }

    public function reserve(int $bytes): bool
    {
        if ($bytes < 0) {
            throw new \InvalidArgumentException('Outgoing reservation must not be negative.');
        }

        if ($bytes > $this->limit - $this->used) {
            return false;
        }

        $this->used += $bytes;
        return true;
    }

    public function release(int $bytes): void
    {
        if ($bytes < 0 || $bytes > $this->used) {
            throw new \LogicException('Invalid outgoing queue reservation release.');
        }
        $this->used -= $bytes;
    }
}
