<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

final class LoopWatcher implements LoopWatcherInterface
{
    public private(set) bool $enabled = true;

    public function __construct(
        public private(set) readonly int|string $id,
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
        if ($this->enabled) {
            ($this->callback)();
        }
    }
}
