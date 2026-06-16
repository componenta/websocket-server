<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final readonly class StreamSocketSelector implements SocketSelectorInterface
{
    public function select(
        iterable $listeners,
        iterable $read,
        iterable $write,
        int $timeoutUsec,
    ): SocketSelectionInterface {
        $readResources = [];
        $writeResources = [];
        $listenerSockets = [];
        $readSockets = [];
        $writeSockets = [];

        foreach ($listeners as $listener) {
            if (!$listener instanceof StreamSocketListenerInterface) {
                throw new \InvalidArgumentException(sprintf(
                    '%s supports only %s.',
                    self::class,
                    StreamSocketListenerInterface::class,
                ));
            }

            $readResources[] = $listener->resource;
            $listenerSockets[(int) $listener->resource] = $listener;
        }

        foreach ($read as $socket) {
            if (!$socket instanceof StreamSocketInterface) {
                throw new \InvalidArgumentException(sprintf('%s supports only %s.', self::class, StreamSocketInterface::class));
            }

            $readResources[] = $socket->resource;
            $readSockets[(int) $socket->resource] = $socket;
        }

        foreach ($write as $socket) {
            if (!$socket instanceof StreamSocketInterface) {
                throw new \InvalidArgumentException(sprintf('%s supports only %s.', self::class, StreamSocketInterface::class));
            }

            $writeResources[] = $socket->resource;
            $writeSockets[(int) $socket->resource] = $socket;
        }

        $except = null;
        $changed = @stream_select($readResources, $writeResources, $except, 0, $timeoutUsec);

        if ($changed === false) {
            throw new \RuntimeException('Unable to select WebSocket sockets.');
        }

        $readableListeners = [];
        $readable = [];
        $writable = [];

        foreach ($readResources as $resource) {
            $key = (int) $resource;

            if (isset($listenerSockets[$key])) {
                $readableListeners[$listenerSockets[$key]->id] = $listenerSockets[$key];
                continue;
            }

            if (isset($readSockets[$key])) {
                $readable[$readSockets[$key]->id] = $readSockets[$key];
            }
        }

        foreach ($writeResources as $resource) {
            $key = (int) $resource;

            if (isset($writeSockets[$key])) {
                $writable[$writeSockets[$key]->id] = $writeSockets[$key];
            }
        }

        return new SocketSelection($readableListeners, $readable, $writable);
    }
}
