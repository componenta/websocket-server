<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Loop\EventLoopInterface;
use Componenta\WebSocket\Loop\LoopTimerInterface;
use Componenta\WebSocket\Loop\LoopWatcherInterface;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;

/** @internal Owns the I/O registrations and deadline of one session. */
final class SessionWatchers
{
    private ?LoopWatcherInterface $reader = null;
    private ?LoopWatcherInterface $writer = null;
    private ?LoopTimerInterface $timer = null;

    public function watchRead(EventLoopInterface $loop, SocketInterface|SocketListenerInterface $socket, callable $callback): void
    {
        if ($this->reader?->enabled !== true) {
            $this->reader = $loop->onReadable($socket, $callback);
        }
    }

    public function watchWrite(EventLoopInterface $loop, SocketInterface $socket, callable $callback): void
    {
        if ($this->writer?->enabled !== true) {
            $this->writer = $loop->onWritable($socket, $callback);
        }
    }

    public function schedule(EventLoopInterface $loop, int $milliseconds, callable $callback): void
    {
        if ($this->timer?->enabled === true) {
            return;
        }

        $this->timer = $loop->delay($milliseconds, function () use ($callback): void {
            $this->timer = null;
            $callback();
        });
    }

    public function cancelRead(): void
    {
        $watcher = $this->reader;
        $this->reader = null;
        $watcher?->cancel();
    }

    public function cancelWrite(): void
    {
        $watcher = $this->writer;
        $this->writer = null;
        $watcher?->cancel();
    }

    /** @return list<\Throwable> Cancellation failures, without skipping other registrations. */
    public function cancelAll(): array
    {
        $registrations = [$this->reader, $this->writer, $this->timer];
        $this->reader = $this->writer = $this->timer = null;
        $errors = [];

        foreach ($registrations as $registration) {
            try {
                $registration?->cancel();
            } catch (\Throwable $error) {
                $errors[] = $error;
            }
        }

        return $errors;
    }
}
