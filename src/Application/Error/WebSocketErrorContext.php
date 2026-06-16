<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application\Error;

use Componenta\WebSocket\Connection\ConnectionInterface;

final readonly class WebSocketErrorContext implements WebSocketErrorContextInterface
{
    public function __construct(
        public WebSocketErrorPhase $phase,
        public ?ConnectionInterface $connection,
        public \Throwable $error,
    ) {}
}
