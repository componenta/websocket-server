<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Socket\SocketInterface;

interface PendingConnectionFactoryInterface
{
    public function create(SocketInterface $socket): PendingConnectionInterface;
}
