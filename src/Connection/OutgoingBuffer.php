<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

/** @internal Bounded-size wire chunks; partial writes never copy the complete queue. */
final class OutgoingBuffer
{
    private const int CHUNK_SIZE = 65_536;

    /** @var \SplQueue<string> */
    private \SplQueue $chunks;
    private int $offset = 0;
    public private(set) int $size = 0;

    public function __construct()
    {
        $this->chunks = new \SplQueue();
    }

    public function append(string $bytes): void
    {
        $length = strlen($bytes);
        if ($length === 0) {
            return;
        }

        $cursor = 0;
        if (!$this->chunks->isEmpty()) {
            $tail = $this->chunks->top();
            $available = self::CHUNK_SIZE - strlen($tail);
            if ($available > 0) {
                $take = min($available, $length);
                $this->chunks->pop();
                $tail .= substr($bytes, 0, $take);
                $this->chunks->enqueue($tail);
                $cursor = $take;
            }
        }

        while ($cursor < $length) {
            $chunk = substr($bytes, $cursor, self::CHUNK_SIZE);
            $this->chunks->enqueue($chunk);
            $cursor += strlen($chunk);
        }
        $this->size += $length;
    }

    public function front(): string
    {
        if ($this->chunks->isEmpty()) {
            return '';
        }

        $head = $this->chunks->bottom();
        return $this->offset === 0 ? $head : substr($head, $this->offset);
    }

    public function consume(int $bytes): void
    {
        $available = $this->chunks->isEmpty() ? 0 : strlen($this->chunks->bottom()) - $this->offset;
        if ($bytes < 0 || $bytes > $available) {
            throw new \LogicException('Socket reported an invalid number of written bytes.');
        }
        if ($bytes === 0) {
            return;
        }

        $this->size -= $bytes;
        $this->offset += $bytes;
        if ($this->offset === strlen($this->chunks->bottom())) {
            $this->chunks->dequeue();
            $this->offset = 0;
        }
    }

    public function clear(): void
    {
        $this->chunks = new \SplQueue();
        $this->offset = 0;
        $this->size = 0;
    }
}
