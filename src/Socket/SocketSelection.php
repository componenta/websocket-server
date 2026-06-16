<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final readonly class SocketSelection implements SocketSelectionInterface
{
    /**
     * @param array<int|string, SocketListenerInterface> $readableListeners
     * @param array<int|string, SocketInterface> $readable
     * @param array<int|string, SocketInterface> $writable
     */
    public function __construct(
        public array $readableListeners,
        public array $readable,
        public array $writable,
    ) {}

    public function canReadListener(SocketListenerInterface $listener): bool
    {
        return isset($this->readableListeners[$listener->id]);
    }

    public function canRead(SocketInterface $socket): bool
    {
        return isset($this->readable[$socket->id]);
    }

    public function canWrite(SocketInterface $socket): bool
    {
        return isset($this->writable[$socket->id]);
    }
}
