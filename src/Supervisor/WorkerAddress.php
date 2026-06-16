<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

final readonly class WorkerAddress implements WorkerAddressInterface
{
    public function __construct(
        public string $host,
        public int $port,
    ) {
        if ($host === '') {
            throw new \InvalidArgumentException('WebSocket worker host must not be empty.');
        }

        if ($port < 1 || $port > 65_535) {
            throw new \InvalidArgumentException('WebSocket worker port must be between 1 and 65535.');
        }
    }

    public function toArray(): array
    {
        return [
            'host' => $this->host,
            'port' => $this->port,
        ];
    }
}
