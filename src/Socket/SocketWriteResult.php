<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final readonly class SocketWriteResult
{
    public function __construct(
        public SocketWriteStatus $status,
        public int $writtenBytes = 0,
        public ?string $reason = null,
    ) {}
}
