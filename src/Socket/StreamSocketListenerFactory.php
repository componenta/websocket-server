<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

use Componenta\WebSocket\Config\WebSocketOptionsInterface;

final readonly class StreamSocketListenerFactory implements SocketListenerFactoryInterface
{
    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
    {
        $context = stream_context_create([
            'socket' => [
                'backlog' => $options->backlog,
            ],
        ]);
        $uri = sprintf('tcp://%s:%d', $options->host, $options->port);
        $server = @stream_socket_server(
            $uri,
            $errno,
            $error,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
            $context,
        );

        if (!is_resource($server)) {
            throw new \RuntimeException(sprintf(
                'Unable to start WebSocket server at %s: [%d] %s',
                $uri,
                $errno,
                $error,
            ));
        }

        return new StreamSocketListener($server);
    }
}
