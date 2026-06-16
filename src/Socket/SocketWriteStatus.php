<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Socket;

enum SocketWriteStatus
{
    case WRITTEN;
    case BLOCKED;
    case ERROR;
}
