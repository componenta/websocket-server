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

        $this->report($application, WebSocketErrorPhase::APPLICATION, $connection, $error);
        if ($closeFailure !== null) {
            $this->report($application, WebSocketErrorPhase::WRITE, $connection, $closeFailure);
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
            $this->report($application, WebSocketErrorPhase::APPLICATION, $connection, $e);
        }
    }

    private function report(
        WebSocketApplicationInterface $application,
        WebSocketErrorPhase $phase,
        ConnectionInterface $connection,
        \Throwable $error,
    ): void {
        try {
            $this->failed($application, $this->errors->create($phase, $connection, $error));
        } catch (\Throwable) {
            // Context creation is also best effort. It must not change the
            // outcome of the callback or prevent a secondary error report.
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
