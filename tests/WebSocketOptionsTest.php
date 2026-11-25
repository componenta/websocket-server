<?php

declare(strict_types=1);

use Componenta\Config\Config;
use Componenta\Config\Environment;
use Componenta\WebSocket\Config\ConfigKey;
use Componenta\WebSocket\Config\WebSocketOptions;

describe('WebSocket options contract', function () {
    it('preserves constructor defaults when no settings are provided', function () {
        expect(WebSocketOptions::fromConfig(new Config([], new Environment([]))))
            ->toEqual(new WebSocketOptions());
    });

    it('maps every runtime setting without mixing individual and shared limits', function () {
        $settings = [
            ConfigKey::HOST => '::1',
            ConfigKey::PORT => 9123,
            ConfigKey::PATH => '/events',
            ConfigKey::ALLOWED_ORIGINS => [' https://first.example ', 'https://second.example', 'https://first.example'],
            ConfigKey::MAX_FRAME_PAYLOAD_SIZE => 101,
            ConfigKey::MAX_MESSAGE_PAYLOAD_SIZE => 102,
            ConfigKey::MAX_OUTGOING_BUFFER_SIZE => 103,
            ConfigKey::MAX_PENDING_HANDSHAKE_BYTES => 104,
            ConfigKey::PENDING_HANDSHAKE_TIMEOUT_MS => 105,
            ConfigKey::MAX_PENDING_CONNECTIONS => 106,
            ConfigKey::HEARTBEAT_INTERVAL_MS => 107,
            ConfigKey::PONG_TIMEOUT_MS => 108,
            ConfigKey::IDLE_TIMEOUT_MS => 109,
            ConfigKey::SHUTDOWN_DRAIN_TIMEOUT_MS => 110,
            ConfigKey::MAX_CONNECTIONS => 111,
            ConfigKey::SELECT_TIMEOUT_USEC => 112,
            ConfigKey::BACKLOG => 113,
            ConfigKey::CLOSE_HANDSHAKE_TIMEOUT_MS => 114,
            ConfigKey::MAX_TOTAL_OUTGOING_BUFFER_SIZE => 115,
            ConfigKey::MAX_TOTAL_INCOMING_BUFFER_SIZE => 116,
        ];
        $options = WebSocketOptions::fromConfig(new Config($settings, new Environment([])));

        expect($options)->toEqual(new WebSocketOptions(
            host: '::1',
            port: 9123,
            path: '/events',
            allowedOrigins: ['https://first.example', 'https://second.example'],
            maxFramePayloadSize: 101,
            maxMessagePayloadSize: 102,
            maxOutgoingBufferSize: 103,
            maxPendingHandshakeBytes: 104,
            pendingHandshakeTimeoutMs: 105,
            maxPendingConnections: 106,
            heartbeatIntervalMs: 107,
            pongTimeoutMs: 108,
            idleTimeoutMs: 109,
            shutdownDrainTimeoutMs: 110,
            maxConnections: 111,
            selectTimeoutUsec: 112,
            backlog: 113,
            closeHandshakeTimeoutMs: 114,
            maxTotalOutgoingBufferSize: 115,
            maxTotalIncomingBufferSize: 116,
        ));
    });

    it('rejects invalid limits and malformed origin entries at construction', function (array $arguments) {
        expect(fn () => new WebSocketOptions(...$arguments))->toThrow(InvalidArgumentException::class);
    })->with([
        'port below range' => [['port' => 0]],
        'port above range' => [['port' => 65_536]],
        'relative path' => [['path' => 'ws']],
        'zero frame limit' => [['maxFramePayloadSize' => 0]],
        'zero message limit' => [['maxMessagePayloadSize' => 0]],
        'zero outgoing limit' => [['maxOutgoingBufferSize' => 0]],
        'zero pending handshake size' => [['maxPendingHandshakeBytes' => 0]],
        'zero pending timeout' => [['pendingHandshakeTimeoutMs' => 0]],
        'zero pending count' => [['maxPendingConnections' => 0]],
        'negative heartbeat interval' => [['heartbeatIntervalMs' => -1]],
        'zero pong timeout' => [['pongTimeoutMs' => 0]],
        'negative idle timeout' => [['idleTimeoutMs' => -1]],
        'negative drain timeout' => [['shutdownDrainTimeoutMs' => -1]],
        'zero connection count' => [['maxConnections' => 0]],
        'negative select timeout' => [['selectTimeoutUsec' => -1]],
        'zero backlog' => [['backlog' => 0]],
        'zero close timeout' => [['closeHandshakeTimeoutMs' => 0]],
        'zero shared outgoing limit' => [['maxTotalOutgoingBufferSize' => 0]],
        'zero shared incoming limit' => [['maxTotalIncomingBufferSize' => 0]],
        'non-string origin' => [['allowedOrigins' => [1]]],
        'empty origin' => [['allowedOrigins' => ['']]],
        'untrimmed direct origin' => [['allowedOrigins' => [' https://example.test']]],
    ]);

    it('rejects malformed configured origin entries instead of broadening access', function (array $origins) {
        $config = new Config([ConfigKey::ALLOWED_ORIGINS => $origins], new Environment([]));
        expect(fn () => WebSocketOptions::fromConfig($config))->toThrow(InvalidArgumentException::class);
    })->with([[[1]], [[' ']], [['']]]);

    it('preserves intentional zero timeouts and empty origin allowlist', function () {
        $options = WebSocketOptions::fromConfig(new Config([
            ConfigKey::HEARTBEAT_INTERVAL_MS => 0,
            ConfigKey::IDLE_TIMEOUT_MS => 0,
            ConfigKey::SHUTDOWN_DRAIN_TIMEOUT_MS => 0,
            ConfigKey::SELECT_TIMEOUT_USEC => 0,
            ConfigKey::ALLOWED_ORIGINS => [],
        ], new Environment([])));
        expect($options->heartbeatIntervalMs)->toBe(0)
            ->and($options->idleTimeoutMs)->toBe(0)
            ->and($options->shutdownDrainTimeoutMs)->toBe(0)
            ->and($options->selectTimeoutUsec)->toBe(0)
            ->and($options->allowedOrigins)->toBe([]);
    });
});
