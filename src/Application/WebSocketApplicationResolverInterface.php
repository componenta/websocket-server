<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

interface WebSocketApplicationResolverInterface
{
    public function resolve(mixed $application): WebSocketApplicationInterface;
}
