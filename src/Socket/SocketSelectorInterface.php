<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketSelectorInterface
{
    /**
     * @param iterable<SocketListenerInterface> $listeners
     * @param iterable<SocketInterface> $read
     * @param iterable<SocketInterface> $write
     */
    public function select(
        iterable $listeners,
        iterable $read,
        iterable $write,
        int $timeoutUsec,
    ): SocketSelectionInterface;
}
