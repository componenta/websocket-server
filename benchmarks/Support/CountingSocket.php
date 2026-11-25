<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks\Support;

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;

final class CountingSocket implements SocketInterface
{
    public readonly string $id;
    public readonly string $remoteAddress;
    public bool $eof { get => false; }
    public int $written = 0;

    public function __construct(private readonly int $writeLimit = PHP_INT_MAX)
    {
        $this->id = 'benchmark';
        $this->remoteAddress = '127.0.0.1';
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $count = min(strlen($bytes), $this->writeLimit);
        $this->written += $count;

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, $count);
    }

    public function close(): void {}
}
