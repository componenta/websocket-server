<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface SocketInterface
{
    public int|string $id { get; }

    public string $remoteAddress { get; }

    public bool $eof { get; }

    /** Nonblocking read of at most a positive $maxBytes. Expected I/O failures return ERROR. */
    public function read(int $maxBytes): SocketReadResult;

    /**
     * Nonblocking write. WRITTEN reports the exact number of bytes accepted,
     * in the range from zero to strlen($bytes); other statuses report zero.
     */
    public function write(string $bytes): SocketWriteResult;

    public function close(): void;
}
