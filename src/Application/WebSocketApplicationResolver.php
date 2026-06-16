<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Psr\Container\ContainerInterface;

final readonly class WebSocketApplicationResolver implements WebSocketApplicationResolverInterface
{
    public function __construct(
        private ContainerInterface $container,
        private WebSocketApplicationFactoryInterface $factory,
    ) {}

    public function resolve(mixed $application): WebSocketApplicationInterface
    {
        if ($application instanceof WebSocketApplicationInterface) {
            return $application;
        }

        if (is_string($application) && $this->container->has($application)) {
            $application = $this->container->get($application);
        }

        if ($application instanceof WebSocketApplicationInterface) {
            return $application;
        }

        if ($application instanceof MessageRouterInterface) {
            return $this->factory->fromRouter($application);
        }

        if (is_callable($application)) {
            return $this->factory->fromCallable($application);
        }

        throw new \InvalidArgumentException(sprintf(
            'WebSocket application must implement %s, implement %s, or be callable, %s given.',
            WebSocketApplicationInterface::class,
            MessageRouterInterface::class,
            get_debug_type($application),
        ));
    }
}
