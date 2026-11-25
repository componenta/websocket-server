<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Loop\MonotonicClockInterface;
use Componenta\WebSocket\Socket\SocketInterface;

final readonly class PendingConnectionFactory implements PendingConnectionFactoryInterface
{
    public function __construct(
        private MonotonicClockInterface $clock,
    ) {}

    public function create(SocketInterface $socket): PendingConnectionInterface
    {
        return new PendingConnection($socket, $this->clock->nowMilliseconds());
    }
}
