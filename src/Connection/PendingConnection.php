<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketWriteStatus;

final class PendingConnection implements PendingConnectionInterface
{
    public private(set) string $buffer = '';
    private readonly OutgoingBuffer $outgoingBuffer;

    public bool $ready {
        get => str_contains($this->buffer, "\r\n\r\n");
    }

    public bool $needsWrite {
        get => $this->outgoingBuffer->size > 0;
    }

    public function __construct(
        public private(set) readonly SocketInterface $socket,
        private readonly float $acceptedAtMs,
    ) {
        $this->outgoingBuffer = new OutgoingBuffer();
    }

    public function append(string $bytes): void
    {
        $this->buffer .= $bytes;
    }

    public function queueResponse(string $bytes): void
    {
        $this->outgoingBuffer->append($bytes);
    }

    public function flush(): bool
    {
        if (!$this->needsWrite) {
            return true;
        }

        $written = $this->socket->write($this->outgoingBuffer->front());
        if ($written->status === SocketWriteStatus::ERROR) {
            return false;
        }

        if ($written->status === SocketWriteStatus::WRITTEN) {
            $this->outgoingBuffer->consume($written->writtenBytes);
        }

        return true;
    }

    public function tooLarge(int $maxBytes = 8192): bool
    {
        return strlen($this->buffer) > $maxBytes;
    }

    public function expired(float $nowMs, int $timeoutMs): bool
    {
        return $nowMs - $this->acceptedAtMs >= $timeoutMs;
    }


    private function __clone(): void {}

}
