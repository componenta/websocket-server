<?php

declare(strict_types=1);

use Componenta\WebSocket\Application\CallableMessageRouter;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\RoutedWebSocketApplication;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Loop\MonotonicClock;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Socket\StreamSocketListenerFactory;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Transport\Server;

require dirname(__DIR__, 2) . '/vendor/autoload.php';

// A loopback-only echo endpoint for the external protocol test suite.
// Large-payload and compression cases are covered by separate test policies.
$options = new WebSocketOptions(
    host: '127.0.0.1',
    port: 9001,
    path: '/',
    maxFramePayloadSize: 16_777_216,
    maxMessagePayloadSize: 16_777_216,
    maxOutgoingBufferSize: 20_971_520,
    heartbeatIntervalMs: 0,
    closeHandshakeTimeoutMs: 1_000,
);
$clock = new MonotonicClock();
$errors = new WebSocketErrorContextFactory();
$server = new Server(
    $options,
    new StreamSocketListenerFactory(),
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
            throw new RuntimeException($result->reason ?? 'Echo message rejected.');
        }
    }),
    new InMemoryConnectionRegistry(),
);
$server->run($application);
