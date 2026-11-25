<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

interface MessageAssemblerInterface
{
    /** Payload bytes retained between push() calls. Reset and complete messages release them. */
    public int $bufferedBytes { get; }


    public function push(Frame $frame): ?Message;

    public function reset(): void;
}
