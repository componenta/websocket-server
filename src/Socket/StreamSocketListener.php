<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

final class StreamSocketListener implements StreamSocketListenerInterface
{
    public int|string $id {
        get => (int) $this->resource;
    }

    public function __construct(
        public private(set) readonly mixed $resource,
    ) {
        if (!stream_set_blocking($this->resource, false)) {
            throw new \RuntimeException('Unable to configure a nonblocking WebSocket listener.');
        }
    }

    public function accept(): ?SocketInterface
    {
        if (!is_resource($this->resource)) {
            return null;
        }

        $client = @stream_socket_accept($this->resource, 0, $remoteAddress);

        if ($client === false) {
            return null;
        }

        try {
            return new StreamSocket($client, $remoteAddress ?: '');
        } catch (\Throwable $error) {
            fclose($client);
            throw $error;
        }
    }

    public function close(): void
    {
        if (is_resource($this->resource)) {
            @fclose($this->resource);
        }
    }

    private function __clone(): void {}
}
