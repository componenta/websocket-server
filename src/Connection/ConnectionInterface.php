<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;

interface ConnectionInterface
{
    public string $id { get; }

    public string $remoteAddress { get; }

    public HandshakeRequest $request { get; }

    public ConnectionContextInterface $context { get; set; }

    public ConnectionState $state { get; }

    public ?CloseInfo $closeInfo { get; }

    public function send(Message|string $message): SendResult;

    public function sendText(string $payload): SendResult;

    public function sendBinary(string $payload): SendResult;

    public function close(int $code = CloseCode::NORMAL, string $reason = ''): SendResult;
}
