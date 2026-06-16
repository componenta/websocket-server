<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol\Handshake;

final readonly class HandshakeRequest
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $method,
        public string $target,
        public string $path,
        public array $headers,
    ) {}

    public function header(string $name): ?string
    {
        return $this->headers[strtolower($name)] ?? null;
    }
}
