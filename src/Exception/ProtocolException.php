<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Exception;

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\Message;

final class ProtocolException extends WebSocketException
{
    public function __construct(
        string $message,
        public readonly int $closeCode = CloseCode::PROTOCOL_ERROR,
    ) {
        parent::__construct($message);
    }
}
