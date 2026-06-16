<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

use Psr\Clock\ClockInterface;
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
    private int $nextId = 1;

    public function __construct(
        private readonly SocketSelectorInterface $selector,
        private readonly ClockInterface $clock,
        private readonly int $idleTimeoutUsec = 200_000,
    ) {}

    public function onReadable(SocketInterface|SocketListenerInterface $socket, callable $callback): LoopWatcherInterface
    {
        if ($socket instanceof SocketListenerInterface) {
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
            $this->now() + ($milliseconds / 1000),
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
        $this->running = true;

        while ($this->running) {
            $this->runDeferred();
            $this->runDueTimers();

            if (!$this->hasWork()) {
                break;
            }

            $timeoutUsec = $this->nextTimeoutUsec();

            if ($this->listenerTargets === [] && $this->readTargets === [] && $this->writeTargets === []) {
                usleep($timeoutUsec);
                continue;
            }

            $selection = $this->selector->select(
                $this->listenerTargets,
                $this->readTargets,
                $this->writeTargets,
                $timeoutUsec,
            );

            $this->dispatchSelection($selection);
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

        foreach ($callbacks as $callback) {
            $callback();

            if (!$this->running) {
                break;
            }
        }
    }

    private function runDueTimers(): void
    {
        $now = $this->now();

        foreach ($this->timers as $timer) {
            if (!$timer->enabled || $timer->dueAt > $now) {
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
        if ($this->timers === []) {
            return $this->idleTimeoutUsec;
        }

        $now = $this->now();
        $dueAt = null;

        foreach ($this->timers as $timer) {
            $dueAt = $dueAt === null ? $timer->dueAt : min($dueAt, $timer->dueAt);
        }

        if ($dueAt === null) {
            return $this->idleTimeoutUsec;
        }

        $timerTimeoutUsec = max(0, (int) (($dueAt - $now) * 1_000_000));

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

    private function now(): float
    {
        return (float) $this->clock->now()->format('U.u');
    }
}
