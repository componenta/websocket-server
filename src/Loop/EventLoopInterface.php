<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;

interface EventLoopInterface
{
    public function onReadable(SocketInterface|SocketListenerInterface $socket, callable $callback): LoopWatcherInterface;

    public function onWritable(SocketInterface $socket, callable $callback): LoopWatcherInterface;

    public function delay(int $milliseconds, callable $callback): LoopTimerInterface;

    public function defer(callable $callback): void;

    /**
     * Run until stopped or no work remains. Nested runs are not supported.
     * Callback exceptions propagate; pending deferred callbacks remain scheduled.
     */
    public function run(): void;

    public function stop(): void;
}
