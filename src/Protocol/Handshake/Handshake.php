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

        $headers = $request->headers;

        if (!$this->containsHeaderToken($headers['upgrade'] ?? null, 'websocket')) {
            return $this->reject(400, 'Missing WebSocket upgrade header', $request);
        }

        if (!$this->containsHeaderToken($headers['connection'] ?? null, 'upgrade')) {
            return $this->reject(400, 'Missing connection upgrade token', $request);
        }

        if (($headers['sec-websocket-version'] ?? '') !== '13') {
            return $this->reject(426, 'Upgrade Required', $request, [
                Header::CONNECTION => 'Upgrade, close',
                Header::UPGRADE => 'websocket',
                Header::SEC_WEBSOCKET_VERSION => '13',
            ]);
        }

        $key = $headers['sec-websocket-key'] ?? null;

        if (!$this->isValidKey($key)) {
            return $this->reject(400, 'Invalid WebSocket key', $request);
        }

        $origin = $headers['origin'] ?? null;

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

            // Validate the complete field once, before case normalization or OWS trimming.
            if (preg_match('/^([!#$%&\'*+.^_\x60|~0-9A-Za-z-]+):([\x09\x20-\x7e\x80-\xff]*)\z/', $line, $field) !== 1) {
                return null;
            }

            $name = strtolower($field[1]);
            $value = trim($field[2], " \t");
            if (!isset($headers[$name])) {
                $headers[$name] = $value;
                continue;
            }

            if (in_array($name, ['host', 'origin', 'sec-websocket-key', 'sec-websocket-version', 'content-length'], true)) {
                return null;
            }

            $headers[$name] .= ', ' . $value;
        }

        if (isset($headers['transfer-encoding'])
            || (isset($headers['content-length']) && !preg_match('/^0+\z/', $headers['content-length']))
        ) {
            return null;
        }

        $host = $headers['host'] ?? '';
        if (!$this->isValidAuthority($host)) {
            return null;
        }

        $target = $matches['target'];
        if (str_contains($target, '#')) {
            return null;
        }

        if (str_starts_with($target, '/')) {
            $path = explode('?', $target, 2)[0];
        } else {
            if (preg_match('~\Ahttps?://([^/?#]+)~i', $target, $authority) !== 1
                || !$this->isValidAuthority($authority[1])
            ) {
                return null;
            }
            $parts = parse_url($target);
            if ($parts === false || !isset($parts['host']) || isset($parts['user'])) {
                return null;
            }
            // RFC 9112 section 3.2.2: the request-target is authoritative
            // for an absolute-form request. Do not expose the conflicting Host
            // to application routing or authorization.
            $headers['host'] = $authority[1];
            $path = $parts['path'] ?? '/';
        }

        return new HandshakeRequest($matches['method'], $target, $path, $headers);
    }

    private function isValidAuthority(string $authority): bool
    {
        if ($authority === '') {
            return false;
        }

        // URI reg-name: require complete percent escapes. A raw comma is
        // deliberately excluded to reject ambiguous combined Host fields.
        if ($authority[0] !== '[') {
            return preg_match('/\A(?:[a-z0-9._~!$&\'()*+;=-]++|%[0-9a-f]{2})++(?::[0-9]+)?\z/i', $authority) === 1;
        }

        if (preg_match('/\A\[([^\]]+)\](?::[0-9]+)?\z/', $authority, $match) !== 1) {
            return false;
        }

        return filter_var($match[1], FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) !== false
            || preg_match('/\Av[0-9a-f]+\.[a-z0-9._~!$&\'()*+,;=:-]+\z/i', $match[1]) === 1;
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

        if (strcasecmp($value, $token) === 0) {
            return true;
        }

        foreach (explode(',', $value) as $part) {
            if (strcasecmp(trim($part), $token) === 0) {
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
