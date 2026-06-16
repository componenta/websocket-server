<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Socket\StreamSocket;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContext;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Transport\Server;

final class ThrowingWebSocketApplication implements WebSocketApplicationInterface
{
    public ?WebSocketErrorContextInterface $reported = null;

    public function connected(ConnectionInterface $connection): void {}

    public function received(ConnectionInterface $connection, Message $message): void
    {
        throw new RuntimeException('Message failed.');
    }

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}

    public function failed(WebSocketErrorContextInterface $context): void
    {
        $this->reported = $context;
    }
}

final class ThrowingWebSocketErrorHandler implements WebSocketApplicationInterface
{
    public int $failedCalls = 0;

    public function connected(ConnectionInterface $connection): void {}

    public function received(ConnectionInterface $connection, Message $message): void {}

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}

    public function failed(WebSocketErrorContextInterface $context): void
    {
        $this->failedCalls++;

        throw new RuntimeException('Error handler failed.');
    }
}

function invokerTestConnection(string $id): Connection
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

describe('WebSocket application invoker', function () {
    it('reports application message failures without throwing out of the server loop', function () {
        $application = new ThrowingWebSocketApplication();
        $connection = invokerTestConnection('invoker');
        $invoker = new SafeWebSocketApplicationInvoker(new WebSocketErrorContextFactory());

        $invoker->received($application, $connection, new Message(Opcode::TEXT, 'hello'));

        expect($application->reported)->toBeInstanceOf(WebSocketErrorContextInterface::class)
            ->and($application->reported?->phase)->toBe(WebSocketErrorPhase::APPLICATION)
            ->and($connection->closing)->toBeTrue();

        $connection->socket->close();
    });

    it('swallows failed handler exceptions', function () {
        $application = new ThrowingWebSocketErrorHandler();
        $connection = invokerTestConnection('failed-handler');
        $invoker = new SafeWebSocketApplicationInvoker(new WebSocketErrorContextFactory());

        $invoker->failed(
            $application,
            new WebSocketErrorContext(WebSocketErrorPhase::APPLICATION, $connection, new RuntimeException('Failed.')),
        );

        expect($application->failedCalls)->toBe(1);

        $connection->socket->close();
    });
});
