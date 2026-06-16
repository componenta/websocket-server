<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Supervisor;

final readonly class WorkerMessageTarget implements WorkerMessageTargetInterface
{
    public function __construct(
        public ?string $workerId = null,
        public ?string $connectionId = null,
        public ?string $userId = null,
        public ?string $channel = null,
    ) {
        if ($workerId === null && $connectionId === null && $userId === null && $channel === null) {
            throw new \InvalidArgumentException('WebSocket worker message target must not be empty.');
        }

        foreach ([
            'worker id' => $workerId,
            'connection id' => $connectionId,
            'user id' => $userId,
            'channel' => $channel,
        ] as $name => $value) {
            if ($value === '') {
                throw new \InvalidArgumentException(sprintf('WebSocket worker message target %s must not be empty.', $name));
            }
        }
    }

    public static function worker(string $workerId): self
    {
        return new self(workerId: $workerId);
    }

    public static function connection(string $connectionId): self
    {
        return new self(connectionId: $connectionId);
    }

    public static function user(string $userId): self
    {
        return new self(userId: $userId);
    }

    public static function channel(string $channel): self
    {
        return new self(channel: $channel);
    }

    public function toArray(): array
    {
        return [
            'workerId' => $this->workerId,
            'connectionId' => $this->connectionId,
            'userId' => $this->userId,
            'channel' => $this->channel,
        ];
    }
}
