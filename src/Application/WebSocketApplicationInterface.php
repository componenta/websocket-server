<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

interface WebSocketApplicationInterface
{
    public function connected(ConnectionInterface $connection): void;

    public function received(ConnectionInterface $connection, Message $message): void;

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void;

    public function failed(WebSocketErrorContextInterface $context): void;
}
