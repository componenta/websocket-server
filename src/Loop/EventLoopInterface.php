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

    public function run(): void;

    public function stop(): void;
}
