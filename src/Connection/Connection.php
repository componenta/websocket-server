<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Exception\ProtocolException;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\CloseInitiator;
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
    private readonly OutgoingBuffer $outgoingBuffer;
    private ?\Closure $writeNotifier = null;
    private ?ConnectionInterface $applicationHandle = null;
    private bool $peerCloseReceived = false;
    private bool $protocolFailed = false;
    private ?string $pendingPingPayload = null;
    private int $pendingPingBytes = 0;

    public bool $awaitingPong {
        get => $this->pendingPingPayload !== null;
    }

    public bool $pingSent {
        get => $this->awaitingPong && $this->pendingPingBytes === 0;
    }

    /** A complete closing handshake, or a flushed failure close. */
    public bool $readyToClose {
        get => $this->closing && !$this->needsWrite && ($this->peerCloseReceived || $this->protocolFailed);
    }

    public ConnectionInterface $application {
        get => $this->applicationHandle ??= new ApplicationConnection($this);
    }
    private(set) ConnectionState $state = ConnectionState::OPEN;
    private(set) ?CloseInfo $closeInfo = null;
    private(set) int $receivedPongs = 0;

    public bool $needsWrite {
        get => $this->queuedBytes > 0;
    }

    public int $queuedBytes {
        get => $this->outgoingBuffer->size;
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
        private readonly int $maxOutgoingBufferSize = 4_194_304,
        private readonly ?OutgoingBudget $outgoingBudget = null,
    ) {
        $this->outgoingBuffer = new OutgoingBuffer();
    }

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

        $result = $this->queue(new Frame(Opcode::CLOSE, $this->createClosePayload($code, $reason)), notify: false);

        if (!$result->accepted) {
            return $result;
        }

        $this->state = ConnectionState::CLOSING;
        $this->closeInfo = CloseInfo::local($code, $reason);
        $this->notifyWrite();

        return $result;
    }

    /**
     * @return list<Message>
     */
    public function appendBytes(string $bytes): array
    {
        if ($this->closed || $this->peerCloseReceived || $this->protocolFailed) {
            return [];
        }

        $this->buffer .= $bytes;
        $messages = [];

        while (($frame = $this->codec->decode($this->buffer, true, $this->maxPayloadSize)) !== null) {
            $message = $this->handleFrame($frame);

            if ($message !== null) {
                $messages[] = $message;
            }

            if ($this->peerCloseReceived) {
                $this->buffer = '';
                $this->assembler->reset();
                break;
            }
        }

        return $messages;
    }

    public function ping(string $payload = ''): SendResult
    {
        if (strlen($payload) > 125) {
            throw new \InvalidArgumentException('WebSocket ping payload must be at most 125 bytes.');
        }

        if ($this->awaitingPong) {
            return SendResult::rejected('A WebSocket ping is already outstanding.', $this->queuedBytes);
        }

        $result = $this->queue(new Frame(Opcode::PING, $payload), notify: false);
        if ($result->accepted) {
            $this->pendingPingPayload = $payload;
            $this->pendingPingBytes = $this->queuedBytes;
            $this->notifyWrite();
        }

        return $result;
    }

    public function flush(): bool
    {
        if (!$this->needsWrite) {
            return true;
        }

        $bytes = $this->outgoingBuffer->front();
        $written = $this->socket->write($bytes);

        if ($written->status === SocketWriteStatus::ERROR) {
            return false;
        }

        if ($written->status === SocketWriteStatus::WRITTEN) {
            $this->outgoingBuffer->consume($written->writtenBytes);
            $this->outgoingBudget?->release($written->writtenBytes);
            $this->pendingPingBytes = max(0, $this->pendingPingBytes - $written->writtenBytes);
        }

        return true;
    }

    public function setWriteNotifier(?\Closure $notifier): void
    {
        $this->writeNotifier = $notifier;

        if ($notifier !== null && $this->needsWrite) {
            $notifier();
        }
    }

    public function failProtocol(int $code, string $reason): SendResult
    {
        $this->protocolFailed = true;
        $this->buffer = '';
        $this->assembler->reset();

        return $this->close($code, $reason);
    }

    public function markClosed(?CloseInfo $close = null): void
    {
        if ($this->closed) {
            return;
        }

        $info = $close ?? $this->closeInfo ?? CloseInfo::transport('Connection closed.');
        $clean = $this->peerCloseReceived && !$this->protocolFailed && $this->closing && !$this->needsWrite
            && $info->initiator !== CloseInitiator::TRANSPORT;
        $this->state = ConnectionState::CLOSED;
        $this->closeInfo = $info->withClean($clean);
        $this->writeNotifier = null;
        $this->pendingPingPayload = null;
        $this->pendingPingBytes = 0;
        $this->buffer = '';
        $this->outgoingBudget?->release($this->queuedBytes);
        $this->outgoingBuffer->clear();
        $this->assembler->reset();
    }

    private function handleFrame(Frame $frame): ?Message
    {
        return match ($frame->opcode) {
            Opcode::TEXT,
            Opcode::BINARY,
            Opcode::CONTINUATION => $this->closing ? null : $this->assembler->push($frame),
            Opcode::PING => $this->handlePingFrame($frame),
            Opcode::PONG => $this->handlePongFrame($frame),
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

    private function handlePongFrame(Frame $frame): ?Message
    {
        $this->receivedPongs++;

        if ($this->pingSent && $frame->payload === $this->pendingPingPayload) {
            $this->pendingPingPayload = null;
        }

        return null;
    }

    private function handleCloseFrame(Frame $frame): ?Message
    {
        $peerClose = $this->parseCloseInfo($frame->payload);
        $this->peerCloseReceived = true;
        $this->closeInfo ??= $peerClose;

        if ($this->state === ConnectionState::OPEN) {
            $result = $this->queue(new Frame(Opcode::CLOSE, $frame->payload), notify: false);

            if (!$result->accepted) {
                $this->closeInfo = CloseInfo::transport('Unable to queue WebSocket close frame.');
                throw new \RuntimeException('Unable to queue WebSocket close frame.');
            }

            $this->state = ConnectionState::CLOSING;
            $this->notifyWrite();
        }

        return null;
    }

    private function notifyWrite(): void
    {
        if ($this->writeNotifier !== null) {
            ($this->writeNotifier)();
        }
    }

    private function queue(Frame $frame, bool $notify = true): SendResult
    {
        if ($this->state === ConnectionState::CLOSED) {
            return SendResult::rejected('Connection is closed.', $this->queuedBytes);
        }

        if ($this->state === ConnectionState::CLOSING && $frame->opcode !== Opcode::CLOSE
            && !($frame->opcode === Opcode::PONG && !$this->peerCloseReceived)
        ) {
            return SendResult::rejected('Connection is closing.', $this->queuedBytes);
        }

        $encodedBytes = $this->codec->encodedSize($frame);
        if ($encodedBytes < 0) {
            throw new \LogicException('Frame codec reported a negative encoded size.');
        }

        if ($encodedBytes > $this->maxOutgoingBufferSize - $this->queuedBytes) {
            return SendResult::rejected('Outgoing WebSocket buffer is full.', $this->queuedBytes);
        }

        if ($frame->opcode === Opcode::TEXT && !$this->isValidUtf8($frame->payload)) {
            return SendResult::rejected('WebSocket text message payload must be valid UTF-8.', $this->queuedBytes);
        }

        if ($this->outgoingBudget !== null && !$this->outgoingBudget->reserve($encodedBytes)) {
            return SendResult::rejected('Server outgoing WebSocket budget is full.', $this->queuedBytes);
        }

        try {
            $encoded = $this->codec->encode($frame);
            if (strlen($encoded) !== $encodedBytes) {
                throw new \LogicException('Frame codec encoded size does not match its reported size.');
            }
            $wasEmpty = !$this->needsWrite;
            $this->outgoingBuffer->append($encoded);
        } catch (\Throwable $error) {
            $this->outgoingBudget?->release($encodedBytes);
            throw $error;
        }

        $queuedBytes = $this->queuedBytes;
        if ($wasEmpty && $notify) {
            $this->notifyWrite();
        }

        return SendResult::accepted($queuedBytes);
    }

    private function parseCloseInfo(string $payload): CloseInfo
    {
        $length = strlen($payload);

        if ($length === 0) {
            return CloseInfo::remote(CloseCode::NO_STATUS);
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
            throw new ProtocolException('WebSocket close frame reason must be valid UTF-8.', CloseCode::INVALID_PAYLOAD_DATA);
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

    private function __clone(): void {}

}
