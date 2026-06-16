<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

use Componenta\WebSocket\Exception\ProtocolException;

final class MessageAssembler implements MessageAssemblerInterface
{
    private ?Opcode $fragmentOpcode = null;
    private string $fragmentPayload = '';

    public function __construct(
        private readonly int $maxMessagePayloadSize,
    ) {}

    public function push(Frame $frame): ?Message
    {
        return match ($frame->opcode) {
            Opcode::TEXT,
            Opcode::BINARY => $this->pushDataFrame($frame),
            Opcode::CONTINUATION => $this->pushContinuationFrame($frame),
            default => throw new ProtocolException('Only WebSocket data frames can be assembled into messages.'),
        };
    }

    public function reset(): void
    {
        $this->fragmentOpcode = null;
        $this->fragmentPayload = '';
    }

    private function pushDataFrame(Frame $frame): ?Message
    {
        if ($this->fragmentOpcode !== null) {
            throw new ProtocolException('WebSocket data frame started before fragmented message completed.');
        }

        $this->assertPayloadSize(strlen($frame->payload));

        if ($frame->final) {
            $this->assertTextPayload($frame->opcode, $frame->payload);

            return new Message($frame->opcode, $frame->payload);
        }

        $this->fragmentOpcode = $frame->opcode;
        $this->fragmentPayload = $frame->payload;

        return null;
    }

    private function pushContinuationFrame(Frame $frame): ?Message
    {
        if ($this->fragmentOpcode === null) {
            throw new ProtocolException('Unexpected WebSocket continuation frame.');
        }

        $this->assertPayloadSize(strlen($this->fragmentPayload) + strlen($frame->payload));
        $this->fragmentPayload .= $frame->payload;

        if (!$frame->final) {
            return null;
        }

        $opcode = $this->fragmentOpcode;
        $payload = $this->fragmentPayload;
        $this->assertTextPayload($opcode, $payload);
        $message = new Message($opcode, $payload);
        $this->reset();

        return $message;
    }

    private function assertPayloadSize(int $bytes): void
    {
        if ($bytes > $this->maxMessagePayloadSize) {
            throw new ProtocolException(
                'WebSocket message payload exceeds configured maximum size.',
                CloseCode::PAYLOAD_TOO_LARGE,
            );
        }
    }

    private function assertTextPayload(Opcode $opcode, string $payload): void
    {
        if ($opcode !== Opcode::TEXT || preg_match('//u', $payload) === 1) {
            return;
        }

        throw new ProtocolException(
            'WebSocket text message payload must be valid UTF-8.',
            CloseCode::INVALID_PAYLOAD_DATA,
        );
    }
}
