<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

interface MessageAssemblerInterface
{
    /**
     * Non-negative payload bytes retained between push() calls.
     * This count excludes frame headers and must not exceed received payload bytes.
     * Reset and complete messages release them.
     */
    public int $bufferedBytes { get; }


    public function push(Frame $frame): ?Message;

    public function reset(): void;
}
