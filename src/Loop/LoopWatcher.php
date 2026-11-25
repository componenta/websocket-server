<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Loop;

final class LoopWatcher implements LoopWatcherInterface
{
    public private(set) bool $enabled = true;

    private ?\Closure $callback;
    private ?\Closure $cancel;

    public function __construct(
        public private(set) readonly int|string $id,
        callable $callback,
        \Closure $cancel,
    ) {
        $this->callback = \Closure::fromCallable($callback);
        $this->cancel = $cancel;
    }

    public function cancel(): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->enabled = false;
        $cancel = $this->cancel;
        $this->callback = null;
        $this->cancel = null;
        $cancel?->__invoke($this);
    }

    public function invoke(): void
    {
        if ($this->enabled) {
            $this->callback?->__invoke();
        }
    }
}
