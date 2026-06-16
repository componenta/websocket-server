<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

enum WorkerState: string
{
    case STARTING = 'starting';
    case RUNNING = 'running';
    case DRAINING = 'draining';
    case STOPPED = 'stopped';
    case FAILED = 'failed';
}
