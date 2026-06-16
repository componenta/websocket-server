<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

enum CloseInitiator: string
{
    case LOCAL = 'local';
    case REMOTE = 'remote';
    case SERVER = 'server';
    case TRANSPORT = 'transport';
}
