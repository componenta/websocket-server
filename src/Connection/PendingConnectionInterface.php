<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Socket\SocketInterface;

interface PendingConnectionInterface
{
    public SocketInterface $socket { get; }

    public string $remoteAddress { get; }

    public string $buffer { get; }

    public bool $ready { get; }

    public bool $needsWrite { get; }

    public \DateTimeImmutable $acceptedAt { get; }

    public \DateTimeImmutable $lastActivityAt { get; }

    public function append(string $bytes, \DateTimeImmutable $now): void;

    public function queueResponse(string $bytes): void;

    public function flush(): bool;

    public function tooLarge(int $maxBytes = 8192): bool;

    public function expired(\DateTimeImmutable $now, int $timeoutMs): bool;
}
