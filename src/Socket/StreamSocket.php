<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final class StreamSocket implements StreamSocketInterface
{
    public int|string $id {
        get => (int) $this->resource;
    }

    public bool $eof {
        get => feof($this->resource);
    }

    public function __construct(
        public private(set) readonly mixed $resource,
        public private(set) readonly string $remoteAddress,
    ) {
        stream_set_blocking($this->resource, false);
    }

    public function read(int $maxBytes): SocketReadResult
    {
        $bytes = @fread($this->resource, $maxBytes);

        if ($bytes === false) {
            return new SocketReadResult(SocketReadStatus::ERROR, reason: 'Unable to read from socket.');
        }

        if ($bytes === '') {
            return new SocketReadResult($this->eof ? SocketReadStatus::CLOSED : SocketReadStatus::EMPTY);
        }

        return new SocketReadResult(SocketReadStatus::DATA, $bytes);
    }

    public function write(string $bytes): SocketWriteResult
    {
        if ($bytes === '') {
            return new SocketWriteResult(SocketWriteStatus::WRITTEN);
        }

        $written = @fwrite($this->resource, $bytes);

        if ($written === false) {
            return new SocketWriteResult(SocketWriteStatus::ERROR, reason: 'Unable to write to socket.');
        }

        if ($written === 0) {
            return new SocketWriteResult(SocketWriteStatus::BLOCKED);
        }

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, $written);
    }

    public function close(): void
    {
        if (is_resource($this->resource)) {
            @fclose($this->resource);
        }
    }
}
