<?php

declare(strict_types=1);

use Componenta\WebSocket\Application\ConnectionRegistryInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\MessageRouterInterface;
use Componenta\WebSocket\Application\NullMessageRouter;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\ConfigProvider as WebSocketConfigProvider;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Loop\EventLoopInterface;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Transport\Server;
use Componenta\WebSocket\Transport\WebSocketServerInterface;
use Componenta\Config\ConfigKey;

describe('WebSocket config provider', function () {
    it('does not depend on the app integration layer', function () {
        $provider = (string) file_get_contents(
            getcwd() . DIRECTORY_SEPARATOR . 'src' . DIRECTORY_SEPARATOR . 'websocket-server'
            . DIRECTORY_SEPARATOR . 'src' . DIRECTORY_SEPARATOR . 'ConfigProvider.php',
        );

        expect($provider)->not->toContain('Componenta\\App\\');
    });

    it('registers WebSocket contracts in the WebSocket provider', function () {
        $config = (new WebSocketConfigProvider())();
        $dependencies = $config[ConfigKey::DEPENDENCIES];

        expect($dependencies[ConfigKey::ALIASES][WebSocketServerInterface::class])->toBe(Server::class)
            ->and($dependencies[ConfigKey::ALIASES][WebSocketOptionsInterface::class])->toBe(WebSocketOptions::class)
            ->and($dependencies[ConfigKey::ALIASES][EventLoopInterface::class])->toBe(SelectEventLoop::class)
            ->and($dependencies[ConfigKey::ALIASES][ConnectionRegistryInterface::class])
            ->toBe(InMemoryConnectionRegistry::class)
            ->and($dependencies[ConfigKey::ALIASES][MessageRouterInterface::class])->toBe(NullMessageRouter::class)
            ->and($dependencies[ConfigKey::ALIASES][WebSocketErrorContextFactoryInterface::class])
            ->toBe(WebSocketErrorContextFactory::class)
            ->and($dependencies[ConfigKey::FACTORIES])->toHaveKey(WebSocketApplicationInterface::class);
    });
});
