<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketSelectionInterface;
use Componenta\WebSocket\Socket\SocketSelectorInterface;

final class SelectEventLoop implements EventLoopInterface
{
    /** @var array<int|string, LoopWatcher> */
    private array $readableListeners = [];

    /** @var array<int|string, LoopWatcher> */
    private array $readableSockets = [];

    /** @var array<int|string, LoopWatcher> */
    private array $writableSockets = [];

    /** @var array<int|string, SocketListenerInterface> */
    private array $listenerTargets = [];

    /** @var array<int|string, SocketInterface> */
    private array $readTargets = [];

    /** @var array<int|string, SocketInterface> */
    private array $writeTargets = [];

    /** @var array<int|string, LoopTimer> */
    private array $timers = [];

    /** @var list<callable(): void> */
    private array $deferred = [];

    private bool $running = false;
    private bool $executing = false;
    private int $nextId = 1;

    public function __construct(
        private readonly SocketSelectorInterface $selector,
        private readonly MonotonicClockInterface $clock,
        private readonly int $idleTimeoutUsec = 200_000,
    ) {}

    public function onReadable(SocketInterface|SocketListenerInterface $socket, callable $callback): LoopWatcherInterface
    {
        if ($socket instanceof SocketListenerInterface) {
            if (($this->listenerTargets[$socket->id] ?? null) !== $socket) {
                $this->selector->assertSelectable($socket);
            }
            ($this->readableListeners[$socket->id] ?? null)?->cancel();
            $watcher = $this->createWatcher($callback, function (LoopWatcher $watcher) use ($socket): void {
                if (($this->readableListeners[$socket->id] ?? null) === $watcher) {
                    unset($this->readableListeners[$socket->id], $this->listenerTargets[$socket->id]);
                }
            });
            $this->readableListeners[$socket->id] = $watcher;
            $this->listenerTargets[$socket->id] = $socket;

            return $watcher;
        }

        if (($this->readTargets[$socket->id] ?? null) !== $socket
            && ($this->writeTargets[$socket->id] ?? null) !== $socket
        ) {
            $this->selector->assertSelectable($socket);
        }
        ($this->readableSockets[$socket->id] ?? null)?->cancel();
        $watcher = $this->createWatcher($callback, function (LoopWatcher $watcher) use ($socket): void {
            if (($this->readableSockets[$socket->id] ?? null) === $watcher) {
                unset($this->readableSockets[$socket->id], $this->readTargets[$socket->id]);
            }
        });
        $this->readableSockets[$socket->id] = $watcher;
        $this->readTargets[$socket->id] = $socket;

        return $watcher;
    }

    public function onWritable(SocketInterface $socket, callable $callback): LoopWatcherInterface
    {
        if (($this->readTargets[$socket->id] ?? null) !== $socket
            && ($this->writeTargets[$socket->id] ?? null) !== $socket
        ) {
            $this->selector->assertSelectable($socket);
        }
        ($this->writableSockets[$socket->id] ?? null)?->cancel();
        $watcher = $this->createWatcher($callback, function (LoopWatcher $watcher) use ($socket): void {
            if (($this->writableSockets[$socket->id] ?? null) === $watcher) {
                unset($this->writableSockets[$socket->id], $this->writeTargets[$socket->id]);
            }
        });
        $this->writableSockets[$socket->id] = $watcher;
        $this->writeTargets[$socket->id] = $socket;

        return $watcher;
    }

    public function delay(int $milliseconds, callable $callback): LoopTimerInterface
    {
        if ($milliseconds < 1) {
            throw new \InvalidArgumentException('Loop delay must be positive.');
        }

        $id = $this->nextId++;
        $timer = new LoopTimer(
            $id,
            $this->clock->nowMilliseconds() + $milliseconds,
            $callback,
            function (LoopTimer $timer): void {
                unset($this->timers[$timer->id]);
            },
        );
        $this->timers[$id] = $timer;

        return $timer;
    }

    public function defer(callable $callback): void
    {
        $this->deferred[] = $callback;
    }

    public function run(): void
    {
        if ($this->executing) {
            throw new \LogicException('Event loop is already running.');
        }

        $this->executing = true;
        $this->running = true;

        try {
            while ($this->running) {
                $this->runDeferred();
                if (!$this->running) {
                    break;
                }

                $this->runDueTimers();
                if (!$this->running || !$this->hasWork()) {
                    break;
                }

                $timeoutUsec = $this->nextTimeoutUsec();

                if ($this->listenerTargets === [] && $this->readTargets === [] && $this->writeTargets === []) {
                    if ($timeoutUsec > 0) {
                        usleep($timeoutUsec);
                    }
                    continue;
                }

                $selection = $this->selector->select(
                    $this->listenerTargets,
                    $this->readTargets,
                    $this->writeTargets,
                    $timeoutUsec,
                );

                if ($this->running) {
                    $this->dispatchSelection($selection);
                }
            }
        } finally {
            $this->running = false;
            $this->executing = false;
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    private function createWatcher(callable $callback, \Closure $cancel): LoopWatcher
    {
        return new LoopWatcher($this->nextId++, $callback, $cancel);
    }

    private function runDeferred(): void
    {
        $callbacks = $this->deferred;
        $this->deferred = [];

        foreach ($callbacks as $index => $callback) {
            $completed = false;
            try {
                $callback();
                $completed = true;
            } finally {
                if (!$completed || !$this->running) {
                    $this->deferred = array_merge(array_slice($callbacks, $index + 1), $this->deferred);
                }
            }

            if (!$this->running) {
                break;
            }
        }
    }

    private function runDueTimers(): void
    {
        $now = $this->clock->nowMilliseconds();

        foreach ($this->timers as $timer) {
            if (!$timer->enabled || $timer->dueAtMs > $now) {
                continue;
            }

            unset($this->timers[$timer->id]);
            $timer->invoke();

            if (!$this->running) {
                break;
            }
        }
    }

    private function hasWork(): bool
    {
        return $this->deferred !== []
            || $this->timers !== []
            || $this->listenerTargets !== []
            || $this->readTargets !== []
            || $this->writeTargets !== [];
    }

    private function nextTimeoutUsec(): int
    {
        if ($this->deferred !== []) {
            return 0;
        }

        if ($this->timers === []) {
            return $this->idleTimeoutUsec;
        }

        $now = $this->clock->nowMilliseconds();
        $dueAt = null;

        foreach ($this->timers as $timer) {
            $dueAt = $dueAt === null ? $timer->dueAtMs : min($dueAt, $timer->dueAtMs);
        }

        if ($dueAt === null) {
            return $this->idleTimeoutUsec;
        }

        $timerTimeoutUsec = max(0, (int) (($dueAt - $now) * 1000));

        return min($this->idleTimeoutUsec, $timerTimeoutUsec);
    }

    private function dispatchSelection(SocketSelectionInterface $selection): void
    {
        foreach ($selection->readableListeners as $listener) {
            $id = $listener->id;
            $watcher = $this->readableListeners[$id] ?? null;

            if ($watcher?->enabled === true && ($this->listenerTargets[$id] ?? null) === $listener) {
                $watcher->invoke();

                if (!$this->running) {
                    return;
                }
            }
        }

        foreach ($selection->readable as $socket) {
            $id = $socket->id;
            $watcher = $this->readableSockets[$id] ?? null;

            if ($watcher?->enabled === true && ($this->readTargets[$id] ?? null) === $socket) {
                $watcher->invoke();

                if (!$this->running) {
                    return;
                }
            }
        }

        foreach ($selection->writable as $socket) {
            $id = $socket->id;
            $watcher = $this->writableSockets[$id] ?? null;

            if ($watcher?->enabled === true && ($this->writeTargets[$id] ?? null) === $socket) {
                $watcher->invoke();

                if (!$this->running) {
                    return;
                }
            }
        }
    }

}
