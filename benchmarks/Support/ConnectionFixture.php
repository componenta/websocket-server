<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks\Support;

use Componenta\WebSocket\Connection\BufferBudget;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\MessageAssembler;

final readonly class ConnectionFixture
{
    private FrameCodec $codec;
    private HandshakeRequest $request;
    private ?BufferBudget $incomingBudget;
    private ?BufferBudget $outgoingBudget;

    public function __construct()
    {
        $this->codec = new FrameCodec();
        $this->request = new HandshakeRequest('GET', '/ws', '/ws', []);
        $withBudget = class_exists(BufferBudget::class);
        $this->incomingBudget = $withBudget ? new BufferBudget(67_108_864) : null;
        $this->outgoingBudget = $withBudget ? new BufferBudget(67_108_864) : null;
    }

    public function create(CountingSocket $socket): Connection
    {
        $arguments = [
            'socket' => $socket,
            'codec' => $this->codec,
            'assembler' => new MessageAssembler(1_048_576),
            'maxPayloadSize' => 1_048_576,
            'id' => 'benchmark',
            'request' => $this->request,
        ];

        if ($this->incomingBudget !== null) {
            $arguments['incomingBudget'] = $this->incomingBudget;
            $arguments['outgoingBudget'] = $this->outgoingBudget;
        }

        return new Connection(...$arguments);
    }
}
