<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

use Componenta\WebSocket\Config\WebSocketOptionsInterface;

interface SocketListenerFactoryInterface
{
    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface;
}
