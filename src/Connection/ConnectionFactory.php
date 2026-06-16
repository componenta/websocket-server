<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Protocol\FrameCodecInterface;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\MessageAssemblerFactoryInterface;
use Componenta\WebSocket\Socket\SocketInterface;

final readonly class ConnectionFactory implements ConnectionFactoryInterface
{
    public function __construct(
        private FrameCodecInterface $codec,
        private WebSocketOptionsInterface $options,
        private MessageAssemblerFactoryInterface $assemblers,
    ) {}

    public function create(
        SocketInterface $socket,
        string $id,
        HandshakeRequest $request,
    ): TransportConnectionInterface {
        return new Connection(
            socket: $socket,
            codec: $this->codec,
            assembler: $this->assemblers->create($this->options->maxMessagePayloadSize),
            maxPayloadSize: $this->options->maxFramePayloadSize,
            id: $id,
            request: $request,
            maxOutgoingBufferSize: $this->options->maxOutgoingBufferSize,
        );
    }
}
