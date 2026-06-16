<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketSelectionInterface
{
    /**
     * @var array<int|string, SocketListenerInterface>
     */
    public array $readableListeners { get; }

    /**
     * @var array<int|string, SocketInterface>
     */
    public array $readable { get; }

    /**
     * @var array<int|string, SocketInterface>
     */
    public array $writable { get; }

    public function canReadListener(SocketListenerInterface $listener): bool;

    public function canRead(SocketInterface $socket): bool;

    public function canWrite(SocketInterface $socket): bool;
}
