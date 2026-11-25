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

    public function connected(WebSocketApplicationInterface $application, ConnectionInterface $connection): bool
    {
        try {
            $application->connected($connection);
            return true;
        } catch (\Throwable $e) {
            return $this->applicationFailure($application, $connection, $e);
        }
    }

    public function received(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        Message $message,
    ): bool {
        try {
            $application->received($connection, $message);
            return true;
        } catch (\Throwable $e) {
            return $this->applicationFailure($application, $connection, $e);
        }
    }

    private function applicationFailure(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        \Throwable $error,
    ): false {
        $closeFailure = null;
        try {
            $connection->close(CloseCode::INTERNAL_ERROR, 'Internal server error');
        } catch (\Throwable $failure) {
            $closeFailure = $failure;
        }

        $this->failed($application, $this->errors->create(WebSocketErrorPhase::APPLICATION, $connection, $error));
        if ($closeFailure !== null) {
            $this->failed($application, $this->errors->create(WebSocketErrorPhase::WRITE, $connection, $closeFailure));
        }

        return false;
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
