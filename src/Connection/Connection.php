<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Exception\ProtocolException;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodecInterface;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerInterface;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketWriteStatus;

final class Connection implements TransportConnectionInterface
{
    private string $buffer = '';
    private string $outgoingBuffer = '';
    private int $outgoingOffset = 0;
    private(set) ConnectionState $state = ConnectionState::OPEN;
    private(set) ?CloseInfo $closeInfo = null;
    private(set) int $receivedPongs = 0;

    public bool $needsWrite {
        get => $this->queuedBytes > 0;
    }

    public int $queuedBytes {
        get => strlen($this->outgoingBuffer) - $this->outgoingOffset;
    }

    public bool $closing {
        get => $this->state === ConnectionState::CLOSING;
    }

    public bool $closed {
        get => $this->state === ConnectionState::CLOSED;
    }

    public function __construct(
        private(set) readonly SocketInterface $socket,
        private readonly FrameCodecInterface $codec,
        private readonly MessageAssemblerInterface $assembler,
        private readonly int              $maxPayloadSize,
        private(set) readonly string $id,
        private(set) readonly HandshakeRequest $request,
        public ConnectionContextInterface $context = new ConnectionContext(),
        private readonly int              $maxOutgoingBufferSize = 4_194_304,
    ) {}

    public string $remoteAddress {
        get => $this->socket->remoteAddress;
    }

    public function send(Message|string $message): SendResult
    {
        if ($message instanceof Message) {
            return $this->queue(new Frame($message->opcode, $message->payload));
        }

        return $this->sendText($message);
    }

    public function sendText(string $payload): SendResult
    {
        return $this->queue(new Frame(Opcode::TEXT, $payload));
    }

    public function sendBinary(string $payload): SendResult
    {
        return $this->queue(new Frame(Opcode::BINARY, $payload));
    }

    public function close(int $code = CloseCode::NORMAL, string $reason = ''): SendResult
    {
        if ($this->state !== ConnectionState::OPEN) {
            return SendResult::rejected('Connection is not open.', $this->queuedBytes);
        }

        $result = $this->queue(new Frame(Opcode::CLOSE, $this->createClosePayload($code, $reason)));

        if (!$result->accepted) {
            return $result;
        }

        $this->state = ConnectionState::CLOSING;
        $this->closeInfo = CloseInfo::local($code, $reason);

        return $result;
    }

    /**
     * @return list<Message>
     */
    public function appendBytes(string $bytes): array
    {
        $this->buffer .= $bytes;
        $messages = [];

        while (($frame = $this->codec->decode($this->buffer, true, $this->maxPayloadSize)) !== null) {
            $message = $this->handleFrame($frame);

            if ($message !== null) {
                $messages[] = $message;
            }
        }

        return $messages;
    }

    public function ping(string $payload = ''): SendResult
    {
        if (strlen($payload) > 125) {
            throw new \InvalidArgumentException('WebSocket ping payload must be at most 125 bytes.');
        }

        return $this->queue(new Frame(Opcode::PING, $payload));
    }

    public function flush(): bool
    {
        if (!$this->needsWrite) {
            return true;
        }

        $written = $this->socket->write($this->pendingOutgoingBytes());

        if ($written->status === SocketWriteStatus::ERROR) {
            return false;
        }

        if ($written->writtenBytes > 0) {
            $this->outgoingOffset += min($written->writtenBytes, $this->queuedBytes);

            if (!$this->needsWrite) {
                $this->resetOutgoingBuffer();
            }
        }

        return true;
    }

    public function markClosed(?CloseInfo $close = null): void
    {
        $this->state = ConnectionState::CLOSED;
        $this->closeInfo ??= $close ?? CloseInfo::transport('Connection closed.');
    }

    private function handleFrame(Frame $frame): ?Message
    {
        return match ($frame->opcode) {
            Opcode::TEXT,
            Opcode::BINARY,
            Opcode::CONTINUATION => $this->assembler->push($frame),
            Opcode::PING => $this->handlePingFrame($frame),
            Opcode::PONG => $this->handlePongFrame(),
            Opcode::CLOSE => $this->handleCloseFrame($frame),
        };
    }

    private function handlePingFrame(Frame $frame): ?Message
    {
        $result = $this->queue(new Frame(Opcode::PONG, $frame->payload));

        if (!$result->accepted) {
            throw new \RuntimeException('Unable to queue WebSocket pong frame.');
        }

        return null;
    }

    private function handlePongFrame(): ?Message
    {
        $this->receivedPongs++;

        return null;
    }

    private function handleCloseFrame(Frame $frame): ?Message
    {
        $this->closeInfo ??= $this->parseCloseInfo($frame->payload);

        if ($this->state === ConnectionState::OPEN) {
            $result = $this->queue(new Frame(Opcode::CLOSE, substr($frame->payload, 0, 125)));

            if (!$result->accepted) {
                $this->closeInfo = CloseInfo::transport('Unable to queue WebSocket close frame.');
                throw new \RuntimeException('Unable to queue WebSocket close frame.');
            }

            $this->state = ConnectionState::CLOSING;
        }

        return null;
    }

    private function queue(Frame $frame): SendResult
    {
        if ($this->state === ConnectionState::CLOSED) {
            return SendResult::rejected('Connection is closed.', $this->queuedBytes);
        }

        if ($this->state === ConnectionState::CLOSING && $frame->opcode !== Opcode::CLOSE) {
            return SendResult::rejected('Connection is closing.', $this->queuedBytes);
        }

        $encodedBytes = $this->codec->encodedSize($frame);
        $queuedBytes = $this->queuedBytes + $encodedBytes;

        if ($queuedBytes > $this->maxOutgoingBufferSize) {
            return SendResult::rejected('Outgoing WebSocket buffer is full.', $this->queuedBytes);
        }

        $encoded = $this->codec->encode($frame);

        if (!$this->needsWrite) {
            $this->outgoingBuffer = $encoded;
            $this->outgoingOffset = 0;

            return SendResult::accepted($queuedBytes);
        }

        $this->compactOutgoingBuffer();
        $this->outgoingBuffer .= $encoded;

        return SendResult::accepted($queuedBytes);
    }

    private function pendingOutgoingBytes(): string
    {
        return $this->outgoingOffset === 0
            ? $this->outgoingBuffer
            : substr($this->outgoingBuffer, $this->outgoingOffset);
    }

    private function compactOutgoingBuffer(): void
    {
        if ($this->outgoingOffset === 0) {
            return;
        }

        $this->outgoingBuffer = substr($this->outgoingBuffer, $this->outgoingOffset);
        $this->outgoingOffset = 0;
    }

    private function resetOutgoingBuffer(): void
    {
        $this->outgoingBuffer = '';
        $this->outgoingOffset = 0;
    }

    private function parseCloseInfo(string $payload): CloseInfo
    {
        $length = strlen($payload);

        if ($length === 0) {
            return CloseInfo::remote();
        }

        if ($length === 1) {
            throw new ProtocolException('WebSocket close frame payload is invalid.');
        }

        $code = unpack('n', substr($payload, 0, 2))[1];
        $reason = substr($payload, 2);

        if (!$this->isValidCloseCode($code)) {
            throw new ProtocolException('WebSocket close frame code is invalid.');
        }

        if (!$this->isValidUtf8($reason)) {
            throw new ProtocolException('WebSocket close frame reason must be valid UTF-8.');
        }

        return CloseInfo::remote($code, $reason);
    }

    private function createClosePayload(int $code, string $reason): string
    {
        if (!$this->isValidCloseCode($code)) {
            throw new \InvalidArgumentException('WebSocket close code is invalid.');
        }

        if (strlen($reason) > 123) {
            throw new \InvalidArgumentException('WebSocket close reason must be at most 123 bytes.');
        }

        if (!$this->isValidUtf8($reason)) {
            throw new \InvalidArgumentException('WebSocket close reason must be valid UTF-8.');
        }

        return pack('n', $code) . $reason;
    }

    private function isValidCloseCode(int $code): bool
    {
        if ($code >= 1000 && $code <= 1014) {
            return !in_array($code, [1004, 1005, 1006], true);
        }

        return $code >= 3000 && $code <= 4999;
    }

    private function isValidUtf8(string $value): bool
    {
        return preg_match('//u', $value) === 1;
    }
}
