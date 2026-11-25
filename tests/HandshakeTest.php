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

    it('rejects malformed HTTP upgrade requests', function (string $request) {
        $response = (new Handshake(new WebSocketOptions()))->accept($request);

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(400);
    })->with([
        'HTTP 1.0' => fn () => str_replace('HTTP/1.1', 'HTTP/1.0', webSocketHandshakeRequest()),
        'missing host' => fn () => str_replace("Host: example.test\r\n", '', webSocketHandshakeRequest()),
        'duplicate host' => fn () => str_replace("Host: example.test\r\n", "Host: example.test\r\nHost: another.test\r\n", webSocketHandshakeRequest()),
        'whitespace before colon' => fn () => str_replace('Host:', 'Host :', webSocketHandshakeRequest()),
        'invalid field name' => fn () => str_replace("\r\n\r\n", "\r\nBad Header: value\r\n\r\n", webSocketHandshakeRequest()),
        'duplicate origin' => fn () => str_replace("\r\n\r\n", "\r\nOrigin: https://a.test\r\nOrigin: https://b.test\r\n\r\n", webSocketHandshakeRequest()),
        'invalid field value' => fn () => str_replace("\r\n\r\n", "\r\nX-Test: a\x00b\r\n\r\n", webSocketHandshakeRequest()),
    ]);

    it('accepts a websocket upgrade token in a comma separated list', function () {
        $response = (new Handshake(new WebSocketOptions()))->accept(
            webSocketHandshakeRequest(headers: [Header::UPGRADE => 'other, WebSocket']),
        );

        expect($response->accepted)->toBeTrue();
    });

    it('rejects browser origins when the allowlist is empty', function () {
        $response = (new Handshake(new WebSocketOptions(allowedOrigins: [])))->accept(
            webSocketHandshakeRequest(headers: [Header::ORIGIN => 'https://example.test']),
        );

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(403);
    });
});
