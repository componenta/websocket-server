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

        $completed = $invoker->received($application, $connection, new Message(Opcode::TEXT, 'hello'));

        expect($completed)->toBeFalse()
            ->and($application->reported)->toBeInstanceOf(WebSocketErrorContextInterface::class)
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

    it('reports the original application exception even when scheduling its close fails', function () {
        $application = new class implements WebSocketApplicationInterface {
            public array $errors = [];
            public function connected(ConnectionInterface $connection): void {}
            public function received(ConnectionInterface $connection, Message $message): void
            {
                throw new RuntimeException('original application error');
            }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}
            public function failed(WebSocketErrorContextInterface $context): void
            {
                $this->errors[] = $context;
            }
        };
        $connection = invokerTestConnection('failed-close');
        $connection->setWriteNotifier(static function (): void {
            throw new RuntimeException('write registration failed');
        });
        $invoker = new SafeWebSocketApplicationInvoker(new WebSocketErrorContextFactory());

        try {
            $completed = $invoker->received($application, $connection->application, new Message(Opcode::TEXT, 'hello'));

            expect($completed)->toBeFalse()
                ->and($application->errors)->toHaveCount(2)
                ->and($application->errors[0]->phase)->toBe(WebSocketErrorPhase::APPLICATION)
                ->and($application->errors[0]->error->getMessage())->toBe('original application error')
                ->and($application->errors[1]->phase)->toBe(WebSocketErrorPhase::WRITE)
                ->and($application->errors[1]->error->getMessage())->toBe('write registration failed');
        } finally {
            $connection->markClosed();
            $connection->socket->close();
        }
    });


    it('isolates error context construction failures during all application callbacks', function (string $callback) {
        $factory = new class implements Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface {
            public array $attempts = [];
            public function create(WebSocketErrorPhase $phase, ?ConnectionInterface $connection, Throwable $error): WebSocketErrorContextInterface
            {
                $this->attempts[] = [$phase, $error->getMessage()];
                throw new RuntimeException('error context factory failed');
            }
        };
        $application = new class implements WebSocketApplicationInterface {
            public function connected(ConnectionInterface $connection): void { throw new RuntimeException('connected failed'); }
            public function received(ConnectionInterface $connection, Message $message): void { throw new RuntimeException('received failed'); }
            public function disconnected(ConnectionInterface $connection, CloseInfo $close): void { throw new RuntimeException('disconnected failed'); }
            public function failed(WebSocketErrorContextInterface $context): void { throw new LogicException('No context was constructed.'); }
        };
        $connection = invokerTestConnection('factory-failure');
        $invoker = new SafeWebSocketApplicationInvoker($factory);

        try {
            if ($callback === 'disconnected') {
                $connection->markClosed();
                $invoker->disconnected($application, $connection->application, $connection->closeInfo);
                expect($connection->closed)->toBeTrue();
            } else {
                $completed = $callback === 'connected'
                    ? $invoker->connected($application, $connection->application)
                    : $invoker->received($application, $connection->application, new Message(Opcode::TEXT, 'hello'));
                expect($completed)->toBeFalse()
                    ->and($connection->closing)->toBeTrue()
                    ->and($connection->closeInfo?->code)->toBe(1011);
            }
            expect($factory->attempts)->toBe([[WebSocketErrorPhase::APPLICATION, $callback . ' failed']]);
        } finally {
            $connection->markClosed();
            $connection->socket->close();
        }
    })->with(['connected', 'received', 'disconnected']);

    it('attempts to report a secondary write failure even when the first error context cannot be created', function () {
        $factory = new class implements Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface {
            public function create(WebSocketErrorPhase $phase, ?ConnectionInterface $connection, Throwable $error): WebSocketErrorContextInterface
            {
                if ($phase === WebSocketErrorPhase::APPLICATION) {
                    throw new RuntimeException('context unavailable');
                }
                return new WebSocketErrorContext($phase, $connection, $error);
            }
        };
        $connection = invokerTestConnection('secondary-report');
        $connection->setWriteNotifier(static function (): void {
            throw new RuntimeException('write registration failed');
        });
        $application = new ThrowingWebSocketApplication();
        try {
            expect((new SafeWebSocketApplicationInvoker($factory))->received($application, $connection->application,
                new Message(Opcode::TEXT, 'hello')))->toBeFalse()
                ->and($application->reported?->phase)->toBe(WebSocketErrorPhase::WRITE)
                ->and($application->reported?->error->getMessage())->toBe('write registration failed');
        } finally {
            $connection->markClosed();
            $connection->socket->close();
        }
    });

});
