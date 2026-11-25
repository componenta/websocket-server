<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

/** @internal Bounded-size wire chunks; allocate a queue only when one chunk is insufficient. */
final class OutgoingBuffer
{
    private const int CHUNK_SIZE = 65_536;

    private string $head = '';
    /** @var \SplQueue<string>|null */
    private ?\SplQueue $tail = null;
    private int $offset = 0;
    public private(set) int $size = 0;

    public function append(string $bytes): void
    {
        $length = strlen($bytes);
        if ($length === 0) {
            return;
        }

        if ($this->tail === null && $length <= self::CHUNK_SIZE - strlen($this->head)) {
            $this->head .= $bytes;
            $this->size += $length;
            return;
        }

        $cursor = 0;
        if ($this->tail === null) {
            $take = min(self::CHUNK_SIZE - strlen($this->head), $length);
            if ($take > 0) {
                $this->head .= substr($bytes, 0, $take);
                $cursor = $take;
            }
            $this->tail = new \SplQueue();
        } else {
            $last = $this->tail->top();
            $take = min(self::CHUNK_SIZE - strlen($last), $length);
            if ($take > 0) {
                $this->tail->pop();
                $last .= substr($bytes, 0, $take);
                $this->tail->enqueue($last);
                $cursor = $take;
            }
        }

        while ($cursor < $length) {
            $chunk = substr($bytes, $cursor, self::CHUNK_SIZE);
            $this->tail->enqueue($chunk);
            $cursor += strlen($chunk);
        }
        $this->size += $length;
    }

    public function front(): string
    {
        return $this->offset === 0 ? $this->head : substr($this->head, $this->offset);
    }

    public function consume(int $bytes): void
    {
        $available = strlen($this->head) - $this->offset;
        if ($bytes < 0 || $bytes > $available) {
            throw new \LogicException('Socket reported an invalid number of written bytes.');
        }
        if ($bytes === 0) {
            return;
        }

        $this->size -= $bytes;
        if ($bytes < $available) {
            $this->offset += $bytes;
            return;
        }

        $this->offset = 0;
        if ($this->tail === null) {
            $this->head = '';
            return;
        }

        $this->head = $this->tail->dequeue();
        if ($this->tail->isEmpty()) {
            $this->tail = null;
        }
    }

    public function clear(): void
    {
        $this->head = '';
        $this->tail = null;
        $this->offset = 0;
        $this->size = 0;
    }

    private function __clone(): void {}
}
