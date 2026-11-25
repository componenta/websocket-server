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
    private int $incomingReservedBytes = 0;
    private readonly OutgoingBuffer $outgoingBuffer;
    private ?\Closure $writeNotifier = null;
    private ?ConnectionInterface $applicationHandle = null;
    private bool $peerCloseReceived = false;
    private bool $protocolFailed = false;
    private bool $receiving = false;
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
        private readonly ?BufferBudget $outgoingBudget = null,
        private readonly ?BufferBudget $incomingBudget = null,
    ) {
        if ($assembler->bufferedBytes !== 0) {
            throw new \InvalidArgumentException('Connection requires a fresh message assembler.');
        }

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
        if ($this->protocolFailed || $this->peerCloseReceived) {
            return SendResult::rejected('Connection is closing.', $this->queuedBytes);
        }

        return $this->beginClose($code, $reason);
    }

    private function beginClose(int $code, string $reason): SendResult
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
        $this->assembler->reset();
        $this->syncIncomingReservation();
        $this->notifyWrite();

        return $this->closed ? SendResult::rejected('Connection closed while scheduling write.', $this->queuedBytes) : $result;
    }

    /** @param \Closure(Message): void $onMessage */
    public function receiveBytes(string $bytes, \Closure $onMessage): void
    {
        if ($this->receiving) {
            throw new \LogicException('WebSocket receive is already in progress.');
        }

        if ($this->closed || $this->peerCloseReceived || $this->protocolFailed) {
            return;
        }

        if ($this->incomingBudget !== null) {
            $length = strlen($bytes);
            if (!$this->incomingBudget->reserve($length)) {
                throw new ProtocolException('Incoming WebSocket buffer budget is full.', CloseCode::PAYLOAD_TOO_LARGE);
            }
            $this->incomingReservedBytes += $length;
        }

        $this->receiving = true;
        try {
            $this->buffer .= $bytes;

            while (!$this->closed && !$this->peerCloseReceived && !$this->protocolFailed
                && ($frame = $this->codec->decode($this->buffer, true, $this->maxPayloadSize)) !== null
            ) {
                $message = $this->handleFrame($frame);
                if ($this->peerCloseReceived) {
                    $this->resetIncoming();
                }
                $this->syncIncomingReservation();

                if ($message !== null) {
                    $onMessage($message);
                }
            }
        } finally {
            $this->receiving = false;
            $this->syncIncomingReservation();
        }
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

        return $this->closed && $result->accepted
            ? SendResult::rejected('Connection closed while scheduling write.', $this->queuedBytes)
            : $result;
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
        $this->resetIncoming();

        return $this->beginClose($code, $reason);
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
        $this->outgoingBudget?->release($this->queuedBytes);
        $this->outgoingBuffer->clear();
        $this->resetIncoming();
    }

    private function syncIncomingReservation(): void
    {
        if ($this->incomingBudget === null) {
            return;
        }

        $retained = strlen($this->buffer) + $this->assembler->bufferedBytes;
        if ($retained > $this->incomingReservedBytes) {
            throw new \LogicException('Decoder or assembler retained more bytes than received.');
        }

        $this->incomingBudget->release($this->incomingReservedBytes - $retained);
        $this->incomingReservedBytes = $retained;
    }

    private function resetIncoming(): void
    {
        $this->buffer = '';
        try {
            $this->assembler->reset();
        } finally {
            $this->incomingBudget?->release($this->incomingReservedBytes);
            $this->incomingReservedBytes = 0;
        }
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
            // An acknowledgement does not have to echo the peer's status.
            // Code 1010 is client-only, so acknowledge it without a status.
            $payload = $peerClose->code === CloseCode::MANDATORY_EXTENSION ? '' : $frame->payload;
            $result = $this->queue(new Frame(Opcode::CLOSE, $payload), notify: false);

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

        if ($frame->opcode !== Opcode::CLOSE
            && ($this->protocolFailed || $this->peerCloseReceived
                || ($this->state === ConnectionState::CLOSING && $frame->opcode !== Opcode::PONG))
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

        if ($wasEmpty && $notify) {
            $this->notifyWrite();
            if ($this->closed) {
                return SendResult::rejected('Connection closed while scheduling write.', $this->queuedBytes);
            }
        }

        return SendResult::accepted($this->queuedBytes);
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
        if ($code === CloseCode::MANDATORY_EXTENSION || !$this->isValidCloseCode($code)) {
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
