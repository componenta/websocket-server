<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Config;

final class ConfigKey
{
    public const string HOST = 'componenta.websocket.host';
    public const string PORT = 'componenta.websocket.port';
    public const string PATH = 'componenta.websocket.path';
    public const string APPLICATION = 'componenta.websocket.application';
    public const string HANDLER = 'componenta.websocket.application';
    public const string ALLOWED_ORIGINS = 'componenta.websocket.allowed_origins';
    public const string MAX_FRAME_PAYLOAD_SIZE = 'componenta.websocket.max_frame_payload_size';
    public const string MAX_MESSAGE_PAYLOAD_SIZE = 'componenta.websocket.max_message_payload_size';
    public const string MAX_OUTGOING_BUFFER_SIZE = 'componenta.websocket.max_outgoing_buffer_size';
    public const string MAX_PENDING_HANDSHAKE_BYTES = 'componenta.websocket.max_pending_handshake_bytes';
    public const string PENDING_HANDSHAKE_TIMEOUT_MS = 'componenta.websocket.pending_handshake_timeout_ms';
    public const string MAX_PENDING_CONNECTIONS = 'componenta.websocket.max_pending_connections';
    public const string HEARTBEAT_INTERVAL_MS = 'componenta.websocket.heartbeat_interval_ms';
    public const string PONG_TIMEOUT_MS = 'componenta.websocket.pong_timeout_ms';
    public const string IDLE_TIMEOUT_MS = 'componenta.websocket.idle_timeout_ms';
    public const string SHUTDOWN_DRAIN_TIMEOUT_MS = 'componenta.websocket.shutdown_drain_timeout_ms';
    public const string MAX_CONNECTIONS = 'componenta.websocket.max_connections';
    public const string SELECT_TIMEOUT_USEC = 'componenta.websocket.select_timeout_usec';
    public const string BACKLOG = 'componenta.websocket.backlog';
}
