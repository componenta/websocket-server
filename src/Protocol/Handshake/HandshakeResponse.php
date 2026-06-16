<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol\Handshake;

final readonly class HandshakeResponse
{
    public function __construct(
        public bool $accepted,
        public int $status,
        public string $response,
        public ?HandshakeRequest $request = null,
    ) {}
}
