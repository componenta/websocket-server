<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

final readonly class WebSocketApplicationFactory implements WebSocketApplicationFactoryInterface
{
    public function __construct(
        private ConnectionRegistryInterface $connections,
    ) {}

    public function fromRouter(MessageRouterInterface $router): WebSocketApplicationInterface
    {
        return new RoutedWebSocketApplication($router, $this->connections);
    }

    public function fromCallable(callable $handler): WebSocketApplicationInterface
    {
        return $this->fromRouter(new CallableMessageRouter($handler));
    }
}
