<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface StreamSocketListenerInterface extends SocketListenerInterface
{
    public mixed $resource { get; }
}
