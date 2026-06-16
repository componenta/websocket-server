<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

final readonly class RoutedWebSocketApplication implements WebSocketApplicationInterface
{
    public function __construct(
        private MessageRouterInterface $router,
        private ConnectionRegistryInterface $connections,
    ) {}

    public function connected(ConnectionInterface $connection): void
    {
        $this->connections->add($connection);
    }

    public function received(ConnectionInterface $connection, Message $message): void
    {
        $this->router->route($connection, $message);
    }

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
    {
        $this->connections->remove($connection);
    }

    public function failed(WebSocketErrorContextInterface $context): void {}
}
