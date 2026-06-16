<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketWriteStatus;

final class PendingConnection implements PendingConnectionInterface
{
    public private(set) string $buffer = '';
    private string $outgoingBuffer = '';
    private int $outgoingOffset = 0;

    public bool $ready {
        get => str_contains($this->buffer, "\r\n\r\n");
    }

    public bool $needsWrite {
        get => strlen($this->outgoingBuffer) > $this->outgoingOffset;
    }

    public function __construct(
        public private(set) readonly SocketInterface $socket,
        public private(set) readonly string $remoteAddress,
        public private(set) readonly \DateTimeImmutable $acceptedAt,
        public private(set) \DateTimeImmutable $lastActivityAt,
    ) {}

    public function append(string $bytes, \DateTimeImmutable $now): void
    {
        $this->buffer .= $bytes;
        $this->lastActivityAt = $now;
    }

    public function queueResponse(string $bytes): void
    {
        if (!$this->needsWrite) {
            $this->outgoingBuffer = $bytes;
            $this->outgoingOffset = 0;

            return;
        }

        $this->compactOutgoingBuffer();
        $this->outgoingBuffer .= $bytes;
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
            $this->outgoingOffset += min($written->writtenBytes, strlen($this->outgoingBuffer) - $this->outgoingOffset);

            if (!$this->needsWrite) {
                $this->resetOutgoingBuffer();
            }
        }

        return true;
    }

    public function tooLarge(int $maxBytes = 8192): bool
    {
        return strlen($this->buffer) > $maxBytes;
    }

    public function expired(\DateTimeImmutable $now, int $timeoutMs): bool
    {
        $elapsedMs = ((float) $now->format('U.u') - (float) $this->lastActivityAt->format('U.u')) * 1000;

        return $elapsedMs >= $timeoutMs;
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
}
