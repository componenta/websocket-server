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
        stream_set_blocking($this->resource, false);
    }

    public function accept(): ?SocketInterface
    {
        $client = @stream_socket_accept($this->resource, 0, $remoteAddress);

        if ($client === false) {
            return null;
        }

        return new StreamSocket($client, $remoteAddress ?: '');
    }

    public function close(): void
    {
        if (is_resource($this->resource)) {
            @fclose($this->resource);
        }
    }
}
