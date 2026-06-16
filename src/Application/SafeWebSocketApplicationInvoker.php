<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactoryInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorPhase;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

final readonly class SafeWebSocketApplicationInvoker implements WebSocketApplicationInvokerInterface
{
    public function __construct(
        private WebSocketErrorContextFactoryInterface $errors,
    ) {}

    public function connected(WebSocketApplicationInterface $application, ConnectionInterface $connection): void
    {
        try {
            $application->connected($connection);
        } catch (\Throwable $e) {
            $connection->close(CloseCode::INTERNAL_ERROR, 'Internal server error');
            $this->failed(
                $application,
                $this->errors->create(WebSocketErrorPhase::APPLICATION, $connection, $e),
            );
        }
    }

    public function received(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        Message $message,
    ): void {
        try {
            $application->received($connection, $message);
        } catch (\Throwable $e) {
            $connection->close(CloseCode::INTERNAL_ERROR, 'Internal server error');
            $this->failed(
                $application,
                $this->errors->create(WebSocketErrorPhase::APPLICATION, $connection, $e),
            );
        }
    }

    public function disconnected(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        CloseInfo $close,
    ): void {
        try {
            $application->disconnected($connection, $close);
        } catch (\Throwable $e) {
            $this->failed(
                $application,
                $this->errors->create(WebSocketErrorPhase::APPLICATION, $connection, $e),
            );
        }
    }

    public function failed(WebSocketApplicationInterface $application, WebSocketErrorContextInterface $context): void
    {
        try {
            $application->failed($context);
        } catch (\Throwable) {
            // The server loop must not be controlled by application error handlers.
        }
    }
}
