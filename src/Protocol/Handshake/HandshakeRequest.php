<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol\Handshake;

final readonly class HandshakeRequest
{
    /** @var array<string, string> Lowercase header names. */
    public array $headers;

    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $method,
        public string $target,
        public string $path,
        array $headers,
    ) {
        $snapshot = [];
        foreach ($headers as $name => $value) {
            $snapshot[$name] = $value;
        }
        $this->headers = $snapshot;
    }

    public function header(string $name): ?string
    {
        return $this->headers[strtolower($name)] ?? null;
    }
}
