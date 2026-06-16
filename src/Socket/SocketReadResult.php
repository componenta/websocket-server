<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final readonly class SocketReadResult
{
    public function __construct(
        public SocketReadStatus $status,
        public string $bytes = '',
        public ?string $reason = null,
    ) {}
}
