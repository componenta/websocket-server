<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

final readonly class Frame
{
    public function __construct(
        public Opcode $opcode,
        public string $payload = '',
        public bool $final = true,
    ) {}
}
