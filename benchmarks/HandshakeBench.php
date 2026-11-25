<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('setUp')]
#[Bench\Revs(5_000)]
final class HandshakeBench
{
    private Handshake $handshake;
    private string $valid;
    private string $invalidOrigin;

    public function setUp(): void
    {
        $this->handshake = new Handshake(new WebSocketOptions(allowedOrigins: ['https://example.test']));
        $this->valid = "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
            . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nOrigin: https://example.test\r\n\r\n";
        $this->invalidOrigin = str_replace('https://example.test', 'https://other.test', $this->valid);
    }

    public function benchAcceptUpgrade(): void
    {
        $response = $this->handshake->accept($this->valid);
        if (!$response->accepted || $response->status !== 101) {
            throw new \RuntimeException('Expected accepted upgrade.');
        }
    }

    public function benchRejectOrigin(): void
    {
        $response = $this->handshake->accept($this->invalidOrigin);
        if ($response->accepted || $response->status !== 403) {
            throw new \RuntimeException('Expected rejected origin.');
        }
    }
}
