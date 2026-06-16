<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

final readonly class WorkerDescriptor implements WorkerDescriptorInterface
{
    public function __construct(
        public string $id,
        public WorkerAddressInterface $address,
        public WorkerCapacityInterface $capacity,
        public WorkerState $state,
        public ?int $pid = null,
        public ?\DateTimeImmutable $startedAt = null,
    ) {
        if ($id === '') {
            throw new \InvalidArgumentException('WebSocket worker id must not be empty.');
        }

        if ($pid !== null && $pid < 1) {
            throw new \InvalidArgumentException('WebSocket worker pid must be positive.');
        }
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'address' => $this->address->toArray(),
            'capacity' => $this->capacity->toArray(),
            'state' => $this->state->value,
            'pid' => $this->pid,
            'startedAt' => $this->startedAt?->format(\DateTimeInterface::ATOM),
        ];
    }
}
