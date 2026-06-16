<?php

declare(strict_types=1);

use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketSelection;
use Componenta\WebSocket\Socket\SocketSelectorInterface;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Psr\Clock\ClockInterface;

final class EventLoopTestClock implements ClockInterface
{
    private int $tick = 0;

    public function now(): DateTimeImmutable
    {
        $time = DateTimeImmutable::createFromFormat('U.u', sprintf('1000.%06d', $this->tick * 1000));
        $this->tick++;

        return $time ?: new DateTimeImmutable('@1000');
    }
}

final class EventLoopTestSocket implements SocketInterface
{
    public bool $eof {
        get => false;
    }

    public function __construct(
        public int|string $id,
        public string $remoteAddress = '127.0.0.1:12345',
    ) {}

    public function read(int $maxBytes): SocketReadResult
    {
        return new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        return new SocketWriteResult(SocketWriteStatus::WRITTEN, strlen($bytes));
    }

    public function close(): void {}
}

final class EventLoopTestListener implements SocketListenerInterface
{
    public function __construct(
        public int|string $id,
    ) {}

    public function accept(): ?SocketInterface
    {
        return null;
    }

    public function close(): void {}
}

final class EventLoopTestSelector implements SocketSelectorInterface
{
    public function select(iterable $listeners, iterable $read, iterable $write, int $timeoutUsec): SocketSelection
    {
        $readableListeners = [];
        $readable = [];
        $writable = [];

        foreach ($listeners as $listener) {
            $readableListeners[$listener->id] = $listener;
        }

        foreach ($read as $socket) {
            $readable[$socket->id] = $socket;
        }

        foreach ($write as $socket) {
            $writable[$socket->id] = $socket;
        }

        return new SocketSelection($readableListeners, $readable, $writable);
    }
}

describe('WebSocket event loop', function () {
    it('dispatches readable listener, readable socket, and writable socket watchers', function () {
        $loop = new SelectEventLoop(new EventLoopTestSelector(), new EventLoopTestClock());
        $listener = new EventLoopTestListener('listener');
        $read = new EventLoopTestSocket('read');
        $write = new EventLoopTestSocket('write');
        $events = [];

        $loop->onReadable($listener, function () use (&$events): void {
            $events[] = 'listener';
        });
        $loop->onReadable($read, function () use (&$events): void {
            $events[] = 'read';
        });
        $loop->onWritable($write, function () use (&$events, $loop): void {
            $events[] = 'write';
            $loop->stop();
        });

        $loop->run();

        expect($events)->toBe(['listener', 'read', 'write']);
    });

    it('runs deferred callbacks and timers', function () {
        $loop = new SelectEventLoop(new EventLoopTestSelector(), new EventLoopTestClock());
        $events = [];

        $loop->defer(function () use (&$events): void {
            $events[] = 'defer';
        });
        $loop->delay(1, function () use (&$events, $loop): void {
            $events[] = 'timer';
            $loop->stop();
        });

        $loop->run();

        expect($events)->toBe(['defer', 'timer']);
    });

    it('stops dispatching ready watchers after stop is requested', function () {
        $loop = new SelectEventLoop(new EventLoopTestSelector(), new EventLoopTestClock());
        $listener = new EventLoopTestListener('listener');
        $read = new EventLoopTestSocket('read');
        $events = [];

        $loop->onReadable($listener, function () use (&$events, $loop): void {
            $events[] = 'listener';
            $loop->stop();
        });
        $loop->onReadable($read, function () use (&$events): void {
            $events[] = 'read';
        });

        $loop->run();

        expect($events)->toBe(['listener']);
    });
});
