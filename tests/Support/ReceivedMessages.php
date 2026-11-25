<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Tests\Support;

use Componenta\WebSocket\Connection\TransportConnectionInterface;
use Componenta\WebSocket\Protocol\Message;

final class ReceivedMessages
{
    /** @return list<Message> */
    public static function collect(TransportConnectionInterface $connection, string $bytes): array
    {
        $messages = [];
        $connection->receiveBytes($bytes, static function (Message $message) use (&$messages): void {
            $messages[] = $message;
        });

        return $messages;
    }
}
