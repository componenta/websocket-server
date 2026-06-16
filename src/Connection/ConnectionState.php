<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

enum ConnectionState: string
{
    case OPEN = 'open';
    case CLOSING = 'closing';
    case CLOSED = 'closed';
}
