<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol\Handshake;

use Componenta\Http\Header;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;

final readonly class Handshake implements HandshakeInterface
{
    private const string ACCEPT_SALT = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    public function __construct(
        private WebSocketOptionsInterface $options,
    ) {}

    public function accept(string $rawRequest): HandshakeResponse
    {
        $request = $this->parse($rawRequest);

        if ($request === null) {
            return $this->reject(400, 'Bad Request');
        }

        if ($request->method !== 'GET') {
            return $this->reject(405, 'Method Not Allowed', $request, ['Allow' => 'GET']);
        }

        if ($request->path !== $this->options->path) {
            return $this->reject(404, 'Not Found', $request);
        }

        if (!$this->containsHeaderToken($request->header(Header::UPGRADE), 'websocket')) {
            return $this->reject(400, 'Missing WebSocket upgrade header', $request);
        }

        if (!$this->containsHeaderToken($request->header(Header::CONNECTION), 'upgrade')) {
            return $this->reject(400, 'Missing connection upgrade token', $request);
        }

        if (($request->header(Header::SEC_WEBSOCKET_VERSION) ?? '') !== '13') {
            return $this->reject(426, 'Upgrade Required', $request, [
                Header::CONNECTION => 'Upgrade, close',
                Header::UPGRADE => 'websocket',
                Header::SEC_WEBSOCKET_VERSION => '13',
            ]);
        }

        $key = $request->header(Header::SEC_WEBSOCKET_KEY);

        if (!$this->isValidKey($key)) {
            return $this->reject(400, 'Invalid WebSocket key', $request);
        }

        $origin = $request->header(Header::ORIGIN);

        if ($origin !== null && !$this->originAllowed($origin)) {
            return $this->reject(403, 'Forbidden origin', $request);
        }

        $accept = base64_encode(sha1($key . self::ACCEPT_SALT, true));
        $response = "HTTP/1.1 101 Switching Protocols\r\n"
            . Header::UPGRADE . ": websocket\r\n"
            . Header::CONNECTION . ": Upgrade\r\n"
            . Header::SEC_WEBSOCKET_ACCEPT . ": {$accept}\r\n"
            . "\r\n";

        return new HandshakeResponse(true, 101, $response, $request);
    }

    private function parse(string $rawRequest): ?HandshakeRequest
    {
        $headerEnd = strpos($rawRequest, "\r\n\r\n");

        // This upgrade handler only supports a bodyless request. Never discard
        // unparsed HTTP body or premature WebSocket bytes while switching protocols.
        if ($headerEnd === false || $headerEnd + 4 !== strlen($rawRequest)) {
            return null;
        }

        $lines = explode("\r\n", substr($rawRequest, 0, $headerEnd));
        $requestLine = array_shift($lines);

        if (!is_string($requestLine)
            || !preg_match('/^(?<method>[!#$%&\'*+.^_\x60|~0-9A-Za-z-]+) (?<target>[^\x00-\x20\x7f]+) HTTP\/1\.1\z/', $requestLine, $matches)
        ) {
            return null;
        }

        $headers = [];

        foreach ($lines as $line) {
            if ($line === '') {
                continue;
            }

            $separator = strpos($line, ':');

            if ($separator === false) {
                return null;
            }

            $name = strtolower(substr($line, 0, $separator));
            $value = trim(substr($line, $separator + 1), " \t");

            if (!preg_match('/^[!#$%&\'*+.^_\x60|~0-9a-z-]+\z/', $name)
                || preg_match('/[\x00-\x08\x0a-\x1f\x7f]/', $value)
                || (isset($headers[$name]) && in_array($name, [
                    'host', 'origin', 'sec-websocket-key', 'sec-websocket-version', 'content-length',
                ], true))
            ) {
                return null;
            }

            $headers[$name] = isset($headers[$name])
                ? $headers[$name] . ', ' . $value
                : $value;
        }

        if (isset($headers['transfer-encoding'])
            || (isset($headers['content-length']) && !preg_match('/^0+\z/', $headers['content-length']))
        ) {
            return null;
        }

        $host = $headers['host'] ?? '';
        if (!preg_match('/^(?:\[[0-9a-f:.]+\]|[a-z0-9._~!$&\'()*+;=%-]+)(?::[0-9]+)?\z/i', $host)) {
            return null;
        }

        $target = $matches['target'];
        if (str_contains($target, '#')) {
            return null;
        }

        if (str_starts_with($target, '/')) {
            $path = explode('?', $target, 2)[0];
        } else {
            $parts = parse_url($target);
            if ($parts === false
                || !in_array(strtolower($parts['scheme'] ?? ''), ['http', 'https'], true)
                || !isset($parts['host'])
                || isset($parts['user'])
            ) {
                return null;
            }
            $path = $parts['path'] ?? '/';
        }

        return new HandshakeRequest($matches['method'], $target, $path, $headers);
    }

    /**
     * @param array<string, string> $headers
     */
    private function reject(
        int $status,
        string $reason,
        ?HandshakeRequest $request = null,
        array $headers = [],
    ): HandshakeResponse {
        $body = $reason . "\n";
        $statusLine = sprintf('HTTP/1.1 %d %s', $status, $reason);
        $responseHeaders = [
            Header::CONNECTION => 'close',
            Header::CONTENT_TYPE => 'text/plain; charset=utf-8',
            Header::CONTENT_LENGTH => (string) strlen($body),
            ...$headers,
        ];
        $response = $statusLine . "\r\n";

        foreach ($responseHeaders as $name => $value) {
            $response .= $name . ': ' . $value . "\r\n";
        }

        return new HandshakeResponse(false, $status, $response . "\r\n" . $body, $request);
    }

    private function containsHeaderToken(?string $value, string $token): bool
    {
        if ($value === null) {
            return false;
        }

        foreach (explode(',', $value) as $part) {
            if (strtolower(trim($part)) === strtolower($token)) {
                return true;
            }
        }

        return false;
    }

    private function isValidKey(?string $key): bool
    {
        if ($key === null || $key === '') {
            return false;
        }

        $decoded = base64_decode($key, true);

        return is_string($decoded) && strlen($decoded) === 16 && base64_encode($decoded) === $key;
    }

    private function originAllowed(string $origin): bool
    {
        return in_array('*', $this->options->allowedOrigins, true)
            || in_array($origin, $this->options->allowedOrigins, true);
    }
}
