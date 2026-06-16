<?php

declare(strict_types=1);

namespace Componenta\WebSocket;

use Componenta\WebSocket\Application\ConnectionRegistryInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\MessageRouterInterface;
use Componenta\WebSocket\Application\NullMessageRouter;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Application\WebSocketApplicationFactory;
use Componenta\WebSocket\Application\WebSocketApplicationFactoryInterface;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\WebSocketApplicationInvokerInterface;
use Componenta\WebSocket\Application\WebSocketApplicationResolver;
use Componenta\WebSocket\Application\WebSocketApplicationResolverInterface;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionFactoryInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Connection\PendingConnectionFactoryInterface;
use Componenta\WebSocket\Loop\EventLoopInterface;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\FrameCodecInterface;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Handshake\HandshakeInterface;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Protocol\MessageAssemblerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketSelectorInterface;
use Componenta\WebSocket\Socket\StreamSocketListenerFactory;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Transport\Server;
use Componenta\WebSocket\Transport\WebSocketServerInterface;
use Componenta\Config\Config;
use Componenta\Config\ConfigProvider as BaseConfigProvider;
use Psr\Clock\ClockInterface;
use Psr\Container\ContainerInterface;

class ConfigProvider extends BaseConfigProvider
{
    protected function getFactories(): array
    {
        return [
            WebSocketOptions::class => static fn(ContainerInterface $container): WebSocketOptions
                => WebSocketOptions::fromConfig($container->get(Config::class)),
            Handshake::class => static fn(ContainerInterface $container): Handshake
                => new Handshake($container->get(WebSocketOptionsInterface::class)),
            ConnectionFactory::class => static fn(ContainerInterface $container): ConnectionFactory
                => new ConnectionFactory(
                    $container->get(FrameCodecInterface::class),
                    $container->get(WebSocketOptionsInterface::class),
                    $container->get(MessageAssemblerFactoryInterface::class),
                ),
            PendingConnectionFactory::class => static fn(ContainerInterface $container): PendingConnectionFactory
                => new PendingConnectionFactory(
                    $container->get(ClockInterface::class),
                ),
            SelectEventLoop::class => static fn(ContainerInterface $container): SelectEventLoop
                => new SelectEventLoop(
                    $container->get(SocketSelectorInterface::class),
                    $container->get(ClockInterface::class),
                    $container->get(WebSocketOptionsInterface::class)->selectTimeoutUsec,
                ),
            Server::class => static fn(ContainerInterface $container): Server
                => new Server(
                    $container->get(WebSocketOptionsInterface::class),
                    $container->get(SocketListenerFactoryInterface::class),
                    $container->get(EventLoopInterface::class),
                    $container->get(HandshakeInterface::class),
                    $container->get(ConnectionFactoryInterface::class),
                    $container->get(PendingConnectionFactoryInterface::class),
                    $container->get(WebSocketApplicationInvokerInterface::class),
                    $container->get(WebSocketErrorContextFactoryInterface::class),
                    $container->get(ClockInterface::class),
                ),
            WebSocketApplicationFactory::class => static fn(ContainerInterface $container): WebSocketApplicationFactory
                => new WebSocketApplicationFactory(
                    $container->get(ConnectionRegistryInterface::class),
                ),
            WebSocketApplicationResolver::class => static fn(ContainerInterface $container): WebSocketApplicationResolver
                => new WebSocketApplicationResolver(
                    $container,
                    $container->get(WebSocketApplicationFactoryInterface::class),
                ),
            WebSocketApplicationInterface::class => static fn(ContainerInterface $container): WebSocketApplicationInterface
                => $container
                    ->get(WebSocketApplicationFactoryInterface::class)
                    ->fromRouter($container->get(MessageRouterInterface::class)),
        ];
    }

    protected function getAutowires(): array
    {
        return [
            FrameCodec::class,
            InMemoryConnectionRegistry::class,
            MessageAssemblerFactory::class,
            NullMessageRouter::class,
            SafeWebSocketApplicationInvoker::class,
            SelectEventLoop::class,
            StreamSocketListenerFactory::class,
            StreamSocketSelector::class,
            WebSocketApplicationFactory::class,
            WebSocketErrorContextFactory::class,
        ];
    }

    protected function getAliases(): array
    {
        return [
            ConnectionFactoryInterface::class => ConnectionFactory::class,
            ConnectionRegistryInterface::class => InMemoryConnectionRegistry::class,
            EventLoopInterface::class => SelectEventLoop::class,
            FrameCodecInterface::class => FrameCodec::class,
            HandshakeInterface::class => Handshake::class,
            MessageAssemblerFactoryInterface::class => MessageAssemblerFactory::class,
            MessageRouterInterface::class => NullMessageRouter::class,
            PendingConnectionFactoryInterface::class => PendingConnectionFactory::class,
            SocketListenerFactoryInterface::class => StreamSocketListenerFactory::class,
            SocketSelectorInterface::class => StreamSocketSelector::class,
            WebSocketApplicationFactoryInterface::class => WebSocketApplicationFactory::class,
            WebSocketApplicationInvokerInterface::class => SafeWebSocketApplicationInvoker::class,
            WebSocketApplicationResolverInterface::class => WebSocketApplicationResolver::class,
            WebSocketErrorContextFactoryInterface::class => WebSocketErrorContextFactory::class,
            WebSocketOptionsInterface::class => WebSocketOptions::class,
            WebSocketServerInterface::class => Server::class,
        ];
    }
}
