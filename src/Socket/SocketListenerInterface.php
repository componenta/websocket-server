<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketListenerInterface
{
    public int|string $id { get; }

    public function accept(): ?SocketInterface;

    public function close(): void;
}
