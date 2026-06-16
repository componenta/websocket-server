<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

final readonly class Message
{
    public function __construct(
        public Opcode $opcode,
        public string $payload,
    ) {
        if (!$this->opcode->isData()) {
            throw new \InvalidArgumentException('WebSocket messages must use text or binary opcode.');
        }
    }

    public function isText(): bool
    {
        return $this->opcode === Opcode::TEXT;
    }
}
