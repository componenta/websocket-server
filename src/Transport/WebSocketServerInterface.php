<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Application\WebSocketApplicationInterface;

interface WebSocketServerInterface
{
    public function run(WebSocketApplicationInterface $application): void;

    /** Stop the active run; a stopped server does not stop other loop work. */
    public function stop(): void;
}
