<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketListenerInterface
{
    public int|string $id { get; }

    /** Accept one connection without blocking; null means no connection is ready. */
    public function accept(): ?SocketInterface;

    public function close(): void;
}
