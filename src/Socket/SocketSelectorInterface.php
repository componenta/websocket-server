<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketSelectorInterface
{
    /** Reject an unsupported or unusable target before registering a watcher. */
    public function assertSelectable(SocketInterface|SocketListenerInterface $target): void;

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
