<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Connection\PendingConnectionInterface;
use Componenta\WebSocket\Protocol\Handshake\HandshakeResponse;

/** @internal One accepted socket before application activation. */
final class PendingSession
{
    public readonly SessionWatchers $watchers;
    public private(set) ?HandshakeResponse $response = null;

    public function __construct(public readonly PendingConnectionInterface $connection)
    {
        $this->watchers = new SessionWatchers();
    }

    public function respond(HandshakeResponse $response): void
    {
        if ($this->response !== null) {
            throw new \LogicException('A handshake response has already been queued.');
        }

        $this->watchers->cancelRead();
        $this->connection->queueResponse($response->response);
        $this->response = $response;
    }
}
