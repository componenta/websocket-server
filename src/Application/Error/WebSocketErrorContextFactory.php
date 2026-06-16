<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application\Error;

use Componenta\WebSocket\Connection\ConnectionInterface;

final readonly class WebSocketErrorContextFactory implements WebSocketErrorContextFactoryInterface
{
    public function create(
        WebSocketErrorPhase $phase,
        ?ConnectionInterface $connection,
        \Throwable $error,
    ): WebSocketErrorContextInterface {
        return new WebSocketErrorContext($phase, $connection, $error);
    }
}
