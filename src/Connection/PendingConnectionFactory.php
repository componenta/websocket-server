<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Psr\Clock\ClockInterface;
use Componenta\WebSocket\Socket\SocketInterface;

final readonly class PendingConnectionFactory implements PendingConnectionFactoryInterface
{
    public function __construct(
        private ClockInterface $clock,
    ) {}

    public function create(SocketInterface $socket): PendingConnectionInterface
    {
        $now = $this->clock->now();

        return new PendingConnection($socket, $socket->remoteAddress, $now, $now);
    }
}
