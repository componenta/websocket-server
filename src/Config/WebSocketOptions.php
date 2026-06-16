<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Config;

use Componenta\Config\Config;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;

final readonly class WebSocketOptions implements WebSocketOptionsInterface
{
    /**
     * @param list<string> $allowedOrigins
     */
    public function __construct(
        public string $host = '127.0.0.1',
        public int $port = 8080,
        public string $path = '/ws',
        public array $allowedOrigins = ['*'],
        public int $maxFramePayloadSize = 1_048_576,
        public int $maxMessagePayloadSize = 1_048_576,
        public int $maxOutgoingBufferSize = 4_194_304,
        public int $maxPendingHandshakeBytes = 8192,
        public int $pendingHandshakeTimeoutMs = 10_000,
        public int $maxPendingConnections = 128,
        public int $heartbeatIntervalMs = 30_000,
        public int $pongTimeoutMs = 10_000,
        public int $idleTimeoutMs = 0,
        public int $shutdownDrainTimeoutMs = 1_000,
        public int $maxConnections = 1024,
        public int $selectTimeoutUsec = 200_000,
        public int $backlog = 128,
    ) {
        if ($this->port < 1 || $this->port > 65_535) {
            throw new \InvalidArgumentException('WebSocket port must be between 1 and 65535.');
        }

        if (!str_starts_with($this->path, '/')) {
            throw new \InvalidArgumentException('WebSocket path must start with "/".');
        }

        if ($this->maxFramePayloadSize < 1) {
            throw new \InvalidArgumentException('WebSocket max frame payload size must be positive.');
        }

        if ($this->maxMessagePayloadSize < 1) {
            throw new \InvalidArgumentException('WebSocket max message payload size must be positive.');
        }

        if ($this->maxOutgoingBufferSize < 1) {
            throw new \InvalidArgumentException('WebSocket max outgoing buffer size must be positive.');
        }

        if ($this->maxPendingHandshakeBytes < 1) {
            throw new \InvalidArgumentException('WebSocket max pending handshake bytes must be positive.');
        }

        if ($this->pendingHandshakeTimeoutMs < 1) {
            throw new \InvalidArgumentException('WebSocket pending handshake timeout must be positive.');
        }

        if ($this->maxPendingConnections < 1) {
            throw new \InvalidArgumentException('WebSocket max pending connections must be positive.');
        }

        if ($this->heartbeatIntervalMs < 0) {
            throw new \InvalidArgumentException('WebSocket heartbeat interval must be zero or positive.');
        }

        if ($this->pongTimeoutMs < 1) {
            throw new \InvalidArgumentException('WebSocket pong timeout must be positive.');
        }

        if ($this->idleTimeoutMs < 0) {
            throw new \InvalidArgumentException('WebSocket idle timeout must be zero or positive.');
        }

        if ($this->shutdownDrainTimeoutMs < 0) {
            throw new \InvalidArgumentException('WebSocket shutdown drain timeout must be zero or positive.');
        }

        if ($this->maxConnections < 1) {
            throw new \InvalidArgumentException('WebSocket max connections must be positive.');
        }

        if ($this->selectTimeoutUsec < 0) {
            throw new \InvalidArgumentException('WebSocket select timeout must be zero or positive.');
        }

        if ($this->backlog < 1) {
            throw new \InvalidArgumentException('WebSocket backlog must be positive.');
        }
    }

    public static function fromConfig(Config $config): self
    {
        return new self(
            host: $config->string(ConfigKey::HOST, '127.0.0.1'),
            port: $config->int(ConfigKey::PORT, 8080),
            path: $config->string(ConfigKey::PATH, '/ws'),
            allowedOrigins: self::normalizeOrigins($config->array(ConfigKey::ALLOWED_ORIGINS, ['*'])),
            maxFramePayloadSize: $config->int(ConfigKey::MAX_FRAME_PAYLOAD_SIZE, 1_048_576),
            maxMessagePayloadSize: $config->int(ConfigKey::MAX_MESSAGE_PAYLOAD_SIZE, 1_048_576),
            maxOutgoingBufferSize: $config->int(ConfigKey::MAX_OUTGOING_BUFFER_SIZE, 4_194_304),
            maxPendingHandshakeBytes: $config->int(ConfigKey::MAX_PENDING_HANDSHAKE_BYTES, 8192),
            pendingHandshakeTimeoutMs: $config->int(ConfigKey::PENDING_HANDSHAKE_TIMEOUT_MS, 10_000),
            maxPendingConnections: $config->int(ConfigKey::MAX_PENDING_CONNECTIONS, 128),
            heartbeatIntervalMs: $config->int(ConfigKey::HEARTBEAT_INTERVAL_MS, 30_000),
            pongTimeoutMs: $config->int(ConfigKey::PONG_TIMEOUT_MS, 10_000),
            idleTimeoutMs: $config->int(ConfigKey::IDLE_TIMEOUT_MS, 0),
            shutdownDrainTimeoutMs: $config->int(ConfigKey::SHUTDOWN_DRAIN_TIMEOUT_MS, 1_000),
            maxConnections: $config->int(ConfigKey::MAX_CONNECTIONS, 1024),
            selectTimeoutUsec: $config->int(ConfigKey::SELECT_TIMEOUT_USEC, 200_000),
            backlog: $config->int(ConfigKey::BACKLOG, 128),
        );
    }

    /**
     * @param array<array-key, mixed> $origins
     * @return list<string>
     */
    private static function normalizeOrigins(array $origins): array
    {
        $normalized = [];

        foreach ($origins as $origin) {
            if (!is_string($origin)) {
                continue;
            }

            $origin = trim($origin);

            if ($origin !== '') {
                $normalized[] = $origin;
            }
        }

        return $normalized === [] ? ['*'] : array_values(array_unique($normalized));
    }
}
