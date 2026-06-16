<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol\Handshake;

interface HandshakeInterface
{
    public function accept(string $rawRequest): HandshakeResponse;
}
