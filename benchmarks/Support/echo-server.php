<?php

declare(strict_types=1);

use Componenta\WebSocket\Application\CallableMessageRouter;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\RoutedWebSocketApplication;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Loop\MonotonicClock;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\StreamSocketListener;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Transport\Server;

require dirname(__DIR__) . '/bootstrap.php';

// Bind an ephemeral loopback port without a release/rebind race.
$listenerFactory = new class implements SocketListenerFactoryInterface {
    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
    {
        $resource = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        if (!is_resource($resource)) {
            throw new RuntimeException("Unable to bind benchmark listener: {$error} ({$errno})");
        }

        $address = stream_socket_get_name($resource, false);
        if (!is_string($address)) {
            fclose($resource);
            throw new RuntimeException('Cannot determine benchmark listener address.');
        }

        $listener = new StreamSocketListener($resource);
        fwrite(STDOUT, "READY {$address}\n");
        fflush(STDOUT);

        return $listener;
    }
};

$options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 100);
$clock = new MonotonicClock();
$errors = new WebSocketErrorContextFactory();

$server = new Server(
    $options,
    $listenerFactory,
    new SelectEventLoop(new StreamSocketSelector(), $clock, $options->selectTimeoutUsec),
    new Handshake($options),
    new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
    new PendingConnectionFactory($clock),
    new SafeWebSocketApplicationInvoker($errors),
    $errors,
    $clock,
);

$application = new RoutedWebSocketApplication(
    new CallableMessageRouter(static function (ConnectionInterface $connection, Message $message): void {
        $result = $connection->send($message);
        if (!$result->accepted) {
            throw new RuntimeException($result->reason ?? 'Echo rejected.');
        }
    }),
    new InMemoryConnectionRegistry(),
);

if (function_exists('pcntl_async_signals') && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static fn () => $server->stop());
}

$server->run($application);
