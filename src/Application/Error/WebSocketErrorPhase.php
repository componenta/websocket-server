<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Application\Error;

use Componenta\WebSocket\Protocol\Handshake\Handshake;

enum WebSocketErrorPhase: string
{
    case ACCEPT = 'accept';
    case HANDSHAKE = 'handshake';
    case READ = 'read';
    case WRITE = 'write';
    case APPLICATION = 'application';
    case SELECT = 'select';
    case SHUTDOWN = 'shutdown';
}
