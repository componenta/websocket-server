<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

final readonly class CloseInfo
{
    public function __construct(
        public int $code,
        public string $reason,
        public CloseInitiator $initiator,
        public bool $clean,
    ) {}

    public static function local(int $code = CloseCode::NORMAL, string $reason = ''): self
    {
        return new self($code, $reason, CloseInitiator::LOCAL, true);
    }

    public static function remote(int $code = CloseCode::NORMAL, string $reason = ''): self
    {
        return new self($code, $reason, CloseInitiator::REMOTE, true);
    }

    public static function serverShutdown(): self
    {
        return new self(CloseCode::GOING_AWAY, 'Server shutdown', CloseInitiator::SERVER, true);
    }

    public static function transport(string $reason): self
    {
        return new self(CloseCode::ABNORMAL, $reason, CloseInitiator::TRANSPORT, false);
    }
}
