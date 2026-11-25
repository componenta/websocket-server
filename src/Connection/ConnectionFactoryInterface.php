<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Socket\SocketInterface;

interface ConnectionFactoryInterface
{
    public function create(
        SocketInterface $socket,
        string $id,
        HandshakeRequest $request,
        ?BufferBudget $outgoingBudget = null,
        ?BufferBudget $incomingBudget = null,
    ): TransportConnectionInterface;
}
