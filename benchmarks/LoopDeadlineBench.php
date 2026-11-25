<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Benchmarks\Support\CountingSocket;
use Componenta\WebSocket\Loop\MonotonicClockInterface;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketSelection;
use Componenta\WebSocket\Socket\SocketSelectorInterface;
use PhpBench\Attributes as Bench;

/** Measure I/O dispatch while many deadlines are pending but not due. */
#[Bench\Groups(['checkpoint-runtime'])]
#[Bench\BeforeMethods('setUp')]
#[Bench\AfterMethods('tearDown')]
final class LoopDeadlineBench
{
    private SelectEventLoop $loop;
    private int $callbacks = 0;
    private array $timers = [];
    private \Componenta\WebSocket\Loop\LoopWatcherInterface $watcher;

    public function setUp(): void
    {
        $clock = new class implements MonotonicClockInterface {
            public function nowMilliseconds(): float { return 0.0; }
        };
        $selector = new class implements SocketSelectorInterface {
            public function assertSelectable(SocketInterface|SocketListenerInterface $target): void {}
            public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
            {
                if ($timeoutUsec !== 200_000) {
                    throw new \RuntimeException('Unexpected deadline.');
                }
                return new SocketSelection([], is_array($read) ? $read : iterator_to_array($read), []);
            }
        };
        $this->loop = new SelectEventLoop($selector, $clock);
        $this->watcher = $this->loop->onReadable(new CountingSocket(), function (): void {
            if (++$this->callbacks === 1_000) {
                $this->loop->stop();
            }
        });

        for ($i = 0; $i < 1_000; $i++) {
            $this->timers[] = $this->loop->delay(10_000 + $i, static function (): void {
                throw new \RuntimeException('Future timer fired prematurely.');
            });
        }
    }

    #[Bench\Revs(10)]
    public function benchDispatch1000ReadsWith1000FutureTimers(): void
    {
        $this->callbacks = 0;
        $this->loop->run();
        if ($this->callbacks !== 1_000) {
            throw new \RuntimeException('Read callbacks were lost.');
        }
    }

    public function tearDown(): void
    {
        $this->watcher->cancel();
        foreach ($this->timers as $timer) {
            $timer->cancel();
        }
        $this->timers = [];
    }
}
