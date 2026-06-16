<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

final readonly class SendResult
{
    private function __construct(
        public bool $accepted,
        public int $queuedBytes,
        public ?string $reason = null,
    ) {}

    public static function accepted(int $queuedBytes): self
    {
        return new self(true, $queuedBytes);
    }

    public static function rejected(string $reason, int $queuedBytes = 0): self
    {
        return new self(false, $queuedBytes, $reason);
    }
}
