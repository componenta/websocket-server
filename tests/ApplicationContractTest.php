<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Application\MessageRouterInterface;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Application\RoutedWebSocketApplication;
use Componenta\WebSocket\Socket\StreamSocket;
use Componenta\WebSocket\Application\Error\WebSocketErrorContext;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Transport\Server;

final class RecordingMessageRouter implements MessageRouterInterface
{
    /** @var list<array{connection: string, payload: string}> */
    public array $messages = [];

    public function route(ConnectionInterface $connection, Message $message): void
    {
        $this->messages[] = [
            'connection' => $connection->id,
            'payload' => $message->payload,
        ];
    }
}

function webSocketTestConnection(string $id = 'connection'): Connection
{
    return new Connection(
        socket: new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345'),
        codec: new FrameCodec(),
        assembler: new MessageAssembler(1024),
        maxPayloadSize: 1024,
        id: $id,
        request: new HandshakeRequest('GET', '/ws', '/ws', []),
    );
}

describe('WebSocket application contracts', function () {
    it('keeps connection registry outside the transport server', function () {
        $router = new RecordingMessageRouter();
        $registry = new InMemoryConnectionRegistry();
        $application = new RoutedWebSocketApplication($router, $registry);
        $connection = webSocketTestConnection('abc');

        $application->connected($connection);
        $application->received($connection, new Message(Opcode::TEXT, 'hello'));
        $application->disconnected($connection, CloseInfo::remote());

        expect($registry->count())->toBe(0)
            ->and($router->messages)->toBe([
                ['connection' => 'abc', 'payload' => 'hello'],
            ]);

        $connection->socket->close();
    });

    it('exposes structured immutable error context through an interface', function () {
        $connection = webSocketTestConnection('error-case');
        $error = new RuntimeException('Application failed.');
        $context = new WebSocketErrorContext(WebSocketErrorPhase::APPLICATION, $connection, $error);

        expect($context)->toBeInstanceOf(WebSocketErrorContextInterface::class)
            ->and($context->phase)->toBe(WebSocketErrorPhase::APPLICATION)
            ->and($context->connection)->toBe($connection)
            ->and($context->error)->toBe($error);

        $connection->socket->close();
    });
    it('rejects a non-callable message router at construction rather than on first dispatch', function () {
        expect(fn () => new Componenta\WebSocket\Application\CallableMessageRouter('nonexistent_callable_for_websocket_test'))
            ->toThrow(TypeError::class);
    });

    it('dispatches messages through a valid callable router', function () {
        $received = [];
        $router = new Componenta\WebSocket\Application\CallableMessageRouter(
            static function (ConnectionInterface $connection, Message $message) use (&$received): void {
                $received[] = [$connection->id, $message->payload];
            },
        );
        $connection = webSocketTestConnection('callable');

        try {
            $router->route($connection->application, new Message(Opcode::TEXT, 'hello'));

            expect($received)->toBe([['callable', 'hello']]);
        } finally {
            $connection->markClosed();
            $connection->socket->close();
        }
    });


    it('resolves existing applications, service identifiers, routers and callable handlers', function () {
        $registry = new InMemoryConnectionRegistry();
        $factory = new Componenta\WebSocket\Application\WebSocketApplicationFactory($registry);
        $router = new RecordingMessageRouter();
        $existing = $factory->fromRouter($router);
        $container = new class(['application' => $existing, 'router' => $router]) implements Psr\Container\ContainerInterface {
            public function __construct(private readonly array $services) {}
            public function has(string $id): bool { return array_key_exists($id, $this->services); }
            public function get(string $id): mixed { return $this->services[$id]; }
        };
        $resolver = new Componenta\WebSocket\Application\WebSocketApplicationResolver($container, $factory);

        expect($resolver->resolve($existing))->toBe($existing)
            ->and($resolver->resolve('application'))->toBe($existing)
            ->and(fn () => $resolver->resolve('missing-service'))->toThrow(InvalidArgumentException::class)
            ->and(fn () => $resolver->resolve(42))->toThrow(InvalidArgumentException::class);

        $calls = [];
        $applications = [
            $resolver->resolve($router),
            $resolver->resolve('router'),
            $resolver->resolve(static function (ConnectionInterface $connection, Message $message) use (&$calls): void {
                $calls[] = [$connection->id, $message->payload];
            }),
        ];

        $transport = webSocketTestConnection('resolved');
        $connection = $transport->application;
        try {
            foreach ($applications as $application) {
                $application->connected($connection);
                expect($registry->get('resolved'))->toBe($connection)
                    ->and($registry->toArray())->toBe(['resolved' => $connection]);
                $application->received($connection, new Message(Opcode::TEXT, 'hello'));
                $application->disconnected($connection, CloseInfo::remote());
                expect($registry->count())->toBe(0);
            }
            expect($router->messages)->toBe([
                ['connection' => 'resolved', 'payload' => 'hello'],
                ['connection' => 'resolved', 'payload' => 'hello'],
            ])->and($calls)->toBe([['resolved', 'hello']]);
        } finally {
            $transport->markClosed();
            $transport->socket->close();
        }
    });

    it('removes registry entries by id without exposing a mutable registry array', function () {
        $registry = new InMemoryConnectionRegistry();
        $transport = webSocketTestConnection('registry');
        try {
            $registry->add($transport->application);
            $snapshot = $registry->toArray();
            unset($snapshot['registry']);
            expect($registry->count())->toBe(1)
                ->and($registry->get('registry'))->toBe($transport->application);
            $registry->remove('registry');
            $registry->remove('missing');
            expect($registry->count())->toBe(0)
                ->and($registry->get('registry'))->toBeNull();
        } finally {
            $transport->markClosed();
            $transport->socket->close();
        }
    });

});
