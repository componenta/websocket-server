<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

interface StreamSocketInterface extends SocketInterface
{
    public mixed $resource { get; }
}
