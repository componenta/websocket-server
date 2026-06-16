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
});
