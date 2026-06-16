<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\Message;

final readonly class NullMessageRouter implements MessageRouterInterface
{
    public function route(ConnectionInterface $connection, Message $message): void {}
}
