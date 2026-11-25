<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application;

use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

interface WebSocketApplicationInvokerInterface
{
    /** True when the callback completed; false when it failed. Reporting is best effort. */
    public function connected(WebSocketApplicationInterface $application, ConnectionInterface $connection): bool;

    /** True when the callback completed; false when it failed. Reporting is best effort. */
    public function received(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        Message $message,
    ): bool;

    public function disconnected(
        WebSocketApplicationInterface $application,
        ConnectionInterface $connection,
        CloseInfo $close,
    ): void;

    /** Report a failure without interrupting other sessions or resource cleanup. */
    public function failed(WebSocketApplicationInterface $application, WebSocketErrorContextInterface $context): void;
}
