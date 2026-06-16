<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

final readonly class ConnectionContext implements ConnectionContextInterface
{
    /**
     * @param array<string, mixed> $attributes
     */
    public function __construct(
        private array $attributes = [],
    ) {}

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
        return new self([...$this->attributes, $key => $value]);
    }

    public function without(string $key): ConnectionContextInterface
    {
        $attributes = $this->attributes;
        unset($attributes[$key]);

        return new self($attributes);
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return $this->attributes;
    }
}
