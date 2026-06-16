<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

final readonly class WorkerMessage implements WorkerMessageInterface
{
    /**
     * @param array<string, mixed> $payload
     */
    public function __construct(
        public string $id,
        public string $type,
        public WorkerMessageTargetInterface $target,
        public array $payload,
        public \DateTimeImmutable $createdAt,
    ) {
        if ($id === '') {
            throw new \InvalidArgumentException('WebSocket worker message id must not be empty.');
        }

        if ($type === '') {
            throw new \InvalidArgumentException('WebSocket worker message type must not be empty.');
        }
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'type' => $this->type,
            'target' => $this->target->toArray(),
            'payload' => $this->payload,
            'createdAt' => $this->createdAt->format(\DateTimeInterface::ATOM),
        ];
    }
}
