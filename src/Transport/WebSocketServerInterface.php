<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Application\WebSocketApplicationInterface;

interface WebSocketServerInterface
{
    public function run(WebSocketApplicationInterface $application): void;

    public function stop(): void;
}
