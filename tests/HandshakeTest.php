<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\Http\Header;
use Componenta\WebSocket\Connection\Connection;

function webSocketHandshakeRequest(string $path = '/ws', array $headers = []): string
{
    $headers = [
        Header::HOST => 'example.test',
        Header::UPGRADE => 'websocket',
        Header::CONNECTION => 'keep-alive, Upgrade',
        Header::SEC_WEBSOCKET_KEY => 'dGhlIHNhbXBsZSBub25jZQ==',
        Header::SEC_WEBSOCKET_VERSION => '13',
        ...$headers,
    ];
    $request = "GET {$path} HTTP/1.1\r\n";

    foreach ($headers as $name => $value) {
        $request .= "{$name}: {$value}\r\n";
    }

    return $request . "\r\n";
}

describe('WebSocket handshake', function () {
    it('accepts valid upgrade requests', function () {
        $handshake = new Handshake(new WebSocketOptions(path: '/ws'));

        $response = $handshake->accept(webSocketHandshakeRequest());

        expect($response->accepted)->toBeTrue()
            ->and($response->status)->toBe(101)
            ->and($response->request?->path)->toBe('/ws')
            ->and($response->response)->toContain('Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
    });

    it('rejects requests for another path', function () {
        $handshake = new Handshake(new WebSocketOptions(path: '/events'));

        $response = $handshake->accept(webSocketHandshakeRequest('/ws'));

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(404);
    });

    it('rejects forbidden origins', function () {
        $handshake = new Handshake(new WebSocketOptions(
            path: '/ws',
            allowedOrigins: ['https://allowed.example'],
        ));

        $response = $handshake->accept(webSocketHandshakeRequest(headers: [
            Header::ORIGIN => 'https://blocked.example',
        ]));

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(403);
    });
});
