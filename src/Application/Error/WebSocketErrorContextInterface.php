<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application\Error;

use Componenta\WebSocket\Connection\ConnectionInterface;

interface WebSocketErrorContextInterface
{
    public WebSocketErrorPhase $phase { get; }

    public ?ConnectionInterface $connection { get; }

    public \Throwable $error { get; }
}
