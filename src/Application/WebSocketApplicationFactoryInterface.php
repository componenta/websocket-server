<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

interface WebSocketApplicationFactoryInterface
{
    public function fromRouter(MessageRouterInterface $router): WebSocketApplicationInterface;

    public function fromCallable(callable $handler): WebSocketApplicationInterface;
}
