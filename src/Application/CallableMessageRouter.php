<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\Message;

final readonly class CallableMessageRouter implements MessageRouterInterface
{
    /**
     * @param callable(ConnectionInterface, Message): void $router
     */
    public function __construct(
        private mixed $router,
    ) {}

    public function route(ConnectionInterface $connection, Message $message): void
    {
        ($this->router)($connection, $message);
    }
}
