<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Socket\SocketInterface;

interface PendingConnectionInterface
{
    public SocketInterface $socket { get; }

    public string $buffer { get; }

    public bool $ready { get; }

    public bool $needsWrite { get; }

    public function append(string $bytes): void;

    public function queueResponse(string $bytes): void;

    public function flush(): bool;

    public function tooLarge(int $maxBytes = 8192): bool;

    /** Whether the absolute deadline since acceptance has elapsed, irrespective of activity. */
    public function expired(float $nowMs, int $timeoutMs): bool;
}
