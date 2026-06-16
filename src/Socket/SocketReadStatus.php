<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

enum SocketReadStatus
{
    case DATA;
    case EMPTY;
    case CLOSED;
    case ERROR;
}
