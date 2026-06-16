<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application\Error;

use Componenta\WebSocket\Connection\ConnectionInterface;

interface WebSocketErrorContextFactoryInterface
{
    public function create(
        WebSocketErrorPhase $phase,
        ?ConnectionInterface $connection,
        \Throwable $error,
    ): WebSocketErrorContextInterface;
}
