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

    it('rejects upgrade requests with unsupported HTTP message framing', function (string $extra, string $body) {
        $request = webSocketHandshakeRequest();
        $request = substr($request, 0, -2) . $extra . "\r\n" . $body;

        $response = (new Handshake(new WebSocketOptions()))->accept($request);

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(400);
    })->with([
        'nonempty HTTP body' => ["Content-Length: 3\r\n", 'abc'],
        'body declared without body bytes' => ["Content-Length: 3\r\n", ''],
        'transfer encoding' => ["Transfer-Encoding: chunked\r\n", "0\r\n\r\n"],
        'ambiguous framing' => ["Content-Length: 0\r\nTransfer-Encoding: chunked\r\n", ''],
        'malformed content length' => ["Content-Length: +0\r\n", ''],
        'duplicate content length' => ["Content-Length: 0\r\nContent-Length: 0\r\n", ''],
        'early bytes' => ['', "\x81\x80mask"],
    ]);

    it('accepts an explicitly empty HTTP request body', function () {
        $request = substr(webSocketHandshakeRequest(), 0, -2) . "Content-Length: 0\r\n\r\n";

        expect((new Handshake(new WebSocketOptions()))->accept($request)->accepted)->toBeTrue();
    });

    it('advertises the allowed method on HTTP 405 responses', function () {
        $response = (new Handshake(new WebSocketOptions()))->accept(
            str_replace('GET /ws', 'POST /ws', webSocketHandshakeRequest()),
        );

        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(405)
            ->and(strtolower($response->response))->toContain("\r\nallow: get\r\n");
    });

    it('advertises the supported upgrade and version on HTTP 426 responses', function () {
        $response = (new Handshake(new WebSocketOptions()))->accept(
            webSocketHandshakeRequest(headers: [Header::SEC_WEBSOCKET_VERSION => '12']),
        );

        $wire = strtolower($response->response);
        expect($response->accepted)->toBeFalse()
            ->and($response->status)->toBe(426)
            ->and($wire)->toContain("\r\nupgrade: websocket\r\n")
            ->and($wire)->toContain("\r\nsec-websocket-version: 13\r\n")
            ->and(preg_match('/\r\nconnection: [^\r\n]*\bupgrade\b[^\r\n]*\r\n/', $wire))->toBe(1);
    });


    it('preserves HTTP field grammar across every possible byte in names and values', function () {
        $handshake = new Handshake(new WebSocketOptions());
        $prefix = substr(webSocketHandshakeRequest(), 0, -2);
        $token = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+-.^_" . chr(96) . "|~";

        for ($byte = 0; $byte <= 255; $byte++) {
            $character = chr($byte);
            $name = 'X' . $character . 'Test';
            $response = $handshake->accept($prefix . $name . ": value\r\n\r\n");
            // A colon in the proposed name is the actual field separator:
            // "X:Test: value" is a valid X header, not an invalid field name.
            $validName = str_contains($token, $character) || $character === ':';
            expect($response->accepted)->toBe($validName, 'Field name byte ' . $byte);
            if ($validName && $character !== ':') {
                expect($response->request?->header($name))->toBe('value');
            }

            $response = $handshake->accept($prefix . "X-Test: a{$character}b\r\n\r\n");
            $validValue = $byte === 9 || ($byte >= 32 && $byte !== 127);
            expect($response->accepted)->toBe($validValue, 'Field value byte ' . $byte);
            if ($validValue) {
                expect($response->request?->header('X-Test'))->toBe('a' . $character . 'b');
            }
        }
    });

    it('combines repeated list fields and trims only HTTP optional whitespace', function () {
        $prefix = substr(webSocketHandshakeRequest(), 0, -2);
        $response = (new Handshake(new WebSocketOptions()))->accept(
            $prefix . "X-List:\t first \t\r\nx-list: second\r\nX-Empty:\t \r\n\r\n",
        );
        expect($response->accepted)->toBeTrue()
            ->and($response->request?->header('X-LIST'))->toBe('first, second')
            ->and($response->request?->header('X-Empty'))->toBe('');
    });

});
