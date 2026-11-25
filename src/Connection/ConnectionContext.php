<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

final readonly class ConnectionContext implements ConnectionContextInterface
{
    /** @var array<int|string, mixed> */
    private array $attributes;

    /**
     * @param array<int|string, mixed> $attributes
     */
    public function __construct(array $attributes = [])
    {
        // Copy entries by value to detach direct PHP references. Values such as
        // objects retain their identity; this is not a deep clone.
        $snapshot = [];
        foreach ($attributes as $key => $value) {
            $snapshot[$key] = $value;
        }
        $this->attributes = $snapshot;
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->attributes);
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return array_key_exists($key, $this->attributes) ? $this->attributes[$key] : $default;
    }

    public function with(string $key, mixed $value): ConnectionContextInterface
    {
        $attributes = $this->attributes;
        $attributes[$key] = $value;

        return new self($attributes);
    }

    public function without(string $key): ConnectionContextInterface
    {
        $attributes = $this->attributes;
        unset($attributes[$key]);

        return new self($attributes);
    }

    /**
     * @return array<int|string, mixed>
     */
    public function toArray(): array
    {
        return $this->attributes;
    }
}
