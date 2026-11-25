<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\Arrayable\Arrayable;

/**
 * @extends Arrayable<int|string, mixed>
 */
interface ConnectionContextInterface extends Arrayable
{
    public function has(string $key): bool;

    public function get(string $key, mixed $default = null): mixed;

    public function with(string $key, mixed $value): self;

    public function without(string $key): self;

    /**
     * PHP converts canonical numeric-string keys to integer array keys.
     * Values are not deep-cloned.
     *
     * @return array<int|string, mixed>
     */
    public function toArray(): array;
}
