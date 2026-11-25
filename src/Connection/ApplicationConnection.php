<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;

/** @internal Application-facing handle; does not expose transport capabilities. */
final class ApplicationConnection implements ConnectionInterface
{
    public string $id { get => $this->connection->id; }
    public string $remoteAddress { get => $this->connection->remoteAddress; }
    public HandshakeRequest $request { get => $this->connection->request; }
    public ConnectionState $state { get => $this->connection->state; }
    public ?CloseInfo $closeInfo { get => $this->connection->closeInfo; }
    public ConnectionContextInterface $context {
        get => $this->connection->context;
        set => $this->connection->context = $value;
    }

    public function __construct(private readonly ConnectionInterface $connection) {}

    public function send(Message|string $message): SendResult
    {
        return $this->connection->send($message);
    }

    public function sendText(string $payload): SendResult
    {
        return $this->connection->sendText($payload);
    }

    public function sendBinary(string $payload): SendResult
    {
        return $this->connection->sendBinary($payload);
    }

    public function close(int $code = CloseCode::NORMAL, string $reason = ''): SendResult
    {
        return $this->connection->close($code, $reason);
    }
}
