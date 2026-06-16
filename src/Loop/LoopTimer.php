<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

final class LoopTimer implements LoopTimerInterface
{
    public private(set) bool $enabled = true;

    public function __construct(
        public private(set) readonly int|string $id,
        public private(set) readonly float $dueAt,
        private readonly mixed $callback,
        private readonly \Closure $cancel,
    ) {}

    public function cancel(): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->enabled = false;
        ($this->cancel)($this);
    }

    public function invoke(): void
    {
        if (!$this->enabled) {
            return;
        }

        try {
            ($this->callback)();
        } finally {
            $this->enabled = false;
        }
    }
}
