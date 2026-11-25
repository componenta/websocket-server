<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final readonly class StreamSocketSelector implements SocketSelectorInterface
{
    public function assertSelectable(SocketInterface|SocketListenerInterface $target): void
    {
        if (!$target instanceof StreamSocketInterface && !$target instanceof StreamSocketListenerInterface) {
            throw new \InvalidArgumentException('Stream socket selector requires a stream resource target.');
        }

        if (!is_resource($target->resource)) {
            throw new \InvalidArgumentException('Cannot select a closed stream resource.');
        }

        $read = [$target->resource];
        $write = [];
        // An interrupted probe is not a successful admission check.
        if (!$this->poll($read, $write, 0)) {
            throw new \RuntimeException('Socket admission check was interrupted.');
        }
    }

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

        if ($timeoutUsec < 0) {
            throw new \InvalidArgumentException('Select timeout must not be negative.');
        }

        if ($readResources === [] && $writeResources === []) {
            return new SocketSelection([], [], []);
        }

        if (!$this->poll($readResources, $writeResources, $timeoutUsec)) {
            return new SocketSelection([], [], []);
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

    /**
     * @param array<resource> $read
     * @param array<resource> $write
     */
    private function poll(array &$read, array &$write, int $timeoutUsec): bool
    {
        $warning = null;
        $except = null;

        set_error_handler(static function (int $level, string $message) use (&$warning): bool {
            $warning = $message;
            return true;
        }, E_WARNING);
        try {
            $changed = stream_select(
                $read, $write, $except,
                intdiv($timeoutUsec, 1_000_000),
                $timeoutUsec % 1_000_000,
            );
        } finally {
            restore_error_handler();
        }

        if ($changed === false) {
            if ($warning !== null && (str_contains(strtolower($warning), 'interrupted system call')
                || preg_match('/Unable to select \[(?:4|10004)\]/i', $warning) === 1)
            ) {
                return false;
            }

            throw new \RuntimeException('Unable to select WebSocket sockets.' . ($warning === null ? '' : ' ' . $warning));
        }

        return true;
    }
}
