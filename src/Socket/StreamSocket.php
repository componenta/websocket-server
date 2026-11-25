<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final class StreamSocket implements StreamSocketInterface
{
    public int|string $id {
        get => (int) $this->resource;
    }

    public bool $eof {
        get => !is_resource($this->resource) || feof($this->resource);
    }

    public function __construct(
        public private(set) readonly mixed $resource,
        public private(set) readonly string $remoteAddress,
    ) {
        if (!stream_set_blocking($this->resource, false)) {
            throw new \RuntimeException('Unable to configure a nonblocking WebSocket stream.');
        }
    }

    public function read(int $maxBytes): SocketReadResult
    {
        if (!is_resource($this->resource)) {
            return new SocketReadResult(SocketReadStatus::CLOSED);
        }

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
        if (!is_resource($this->resource)) {
            return new SocketWriteResult(SocketWriteStatus::ERROR, reason: 'Socket is closed.');
        }

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

    private function __clone(): void {}
}
