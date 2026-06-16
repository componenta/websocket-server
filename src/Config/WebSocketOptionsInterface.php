<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Config;

interface WebSocketOptionsInterface
{
    public string $host { get; }

    public int $port { get; }

    public string $path { get; }

    /**
     * @var list<string>
     */
    public array $allowedOrigins { get; }

    public int $maxFramePayloadSize { get; }

    public int $maxMessagePayloadSize { get; }

    public int $maxOutgoingBufferSize { get; }

    public int $maxPendingHandshakeBytes { get; }

    public int $pendingHandshakeTimeoutMs { get; }

    public int $maxPendingConnections { get; }

    public int $heartbeatIntervalMs { get; }

    public int $pongTimeoutMs { get; }

    public int $idleTimeoutMs { get; }

    public int $shutdownDrainTimeoutMs { get; }

    public int $maxConnections { get; }

    public int $selectTimeoutUsec { get; }

    public int $backlog { get; }
}
