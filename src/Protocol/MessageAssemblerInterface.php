<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

interface MessageAssemblerInterface
{
    public function push(Frame $frame): ?Message;

    public function reset(): void;
}
