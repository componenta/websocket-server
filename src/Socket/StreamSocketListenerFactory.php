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
        $host = $options->host;
        if (str_contains($host, ':') && !str_starts_with($host, '[')) {
            $host = '[' . $host . ']';
        }
        $uri = sprintf('tcp://%s:%d', $host, $options->port);
        // Translate the native bind warning into the factory's exception,
        // even if the caller has installed a warning-to-exception handler.
        $warning = null;
        set_error_handler(static function (int $level, string $message) use (&$warning): bool {
            $warning = $message;
            return true;
        }, E_WARNING);
        try {
            $server = stream_socket_server(
                $uri,
                $errno,
                $error,
                STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
                $context,
            );
        } finally {
            restore_error_handler();
        }

        if (!is_resource($server)) {
            throw new \RuntimeException(sprintf(
                'Unable to start WebSocket server at %s: [%d] %s',
                $uri,
                $errno,
                $error !== '' ? $error : ($warning ?? 'Unknown socket error'),
            ));
        }

        try {
            return new StreamSocketListener($server);
        } catch (\Throwable $error) {
            fclose($server);
            throw $error;
        }
    }
}
