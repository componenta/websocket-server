<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketInterface
{
    public int|string $id { get; }

    public string $remoteAddress { get; }

    public bool $eof { get; }

    public function read(int $maxBytes): SocketReadResult;

    public function write(string $bytes): SocketWriteResult;

    public function close(): void;
}
