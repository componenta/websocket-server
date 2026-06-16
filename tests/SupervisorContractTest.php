<?php

declare(strict_types=1);

use Componenta\WebSocket\Supervisor\WorkerAddress;
use Componenta\WebSocket\Supervisor\WorkerAddressInterface;
use Componenta\WebSocket\Supervisor\WorkerCapacity;
use Componenta\WebSocket\Supervisor\WorkerCapacityInterface;
use Componenta\WebSocket\Supervisor\WorkerDescriptor;
use Componenta\WebSocket\Supervisor\WorkerDescriptorInterface;
use Componenta\WebSocket\Supervisor\WorkerMessage;
use Componenta\WebSocket\Supervisor\WorkerMessageInterface;
use Componenta\WebSocket\Supervisor\WorkerMessageTarget;
use Componenta\WebSocket\Supervisor\WorkerMessageTargetInterface;
use Componenta\WebSocket\Supervisor\WorkerState;

describe('WebSocket supervisor contracts', function () {
    it('exposes worker state as immutable property data', function () {
        $startedAt = new DateTimeImmutable('2026-06-07 12:00:00+00:00');
        $address = new WorkerAddress('127.0.0.1', 9001);
        $capacity = new WorkerCapacity(connections: 10, maxConnections: 256);
        $worker = new WorkerDescriptor(
            id: 'worker-1',
            address: $address,
            capacity: $capacity,
            state: WorkerState::RUNNING,
            pid: 1234,
            startedAt: $startedAt,
        );

        expect($address)->toBeInstanceOf(WorkerAddressInterface::class)
            ->and($capacity)->toBeInstanceOf(WorkerCapacityInterface::class)
            ->and($worker)->toBeInstanceOf(WorkerDescriptorInterface::class)
            ->and($worker->id)->toBe('worker-1')
            ->and($worker->address)->toBe($address)
            ->and($worker->capacity->acceptingConnections)->toBeTrue()
            ->and($worker->toArray())->toBe([
                'id' => 'worker-1',
                'address' => [
                    'host' => '127.0.0.1',
                    'port' => 9001,
                ],
                'capacity' => [
                    'connections' => 10,
                    'maxConnections' => 256,
                    'acceptingConnections' => true,
                ],
                'state' => 'running',
                'pid' => 1234,
                'startedAt' => '2026-06-07T12:00:00+00:00',
            ]);
    });

    it('exposes inter-worker messages as immutable property data', function () {
        $target = WorkerMessageTarget::user('user-123');
        $message = new WorkerMessage(
            id: 'message-1',
            type: 'deliver',
            target: $target,
            payload: ['text' => 'hello'],
            createdAt: new DateTimeImmutable('2026-06-07 12:00:00+00:00'),
        );

        expect($target)->toBeInstanceOf(WorkerMessageTargetInterface::class)
            ->and($message)->toBeInstanceOf(WorkerMessageInterface::class)
            ->and($message->target)->toBe($target)
            ->and($message->toArray())->toBe([
                'id' => 'message-1',
                'type' => 'deliver',
                'target' => [
                    'workerId' => null,
                    'connectionId' => null,
                    'userId' => 'user-123',
                    'channel' => null,
                ],
                'payload' => ['text' => 'hello'],
                'createdAt' => '2026-06-07T12:00:00+00:00',
            ]);
    });

    it('rejects invalid worker runtime metadata', function () {
        expect(fn() => new WorkerAddress('', 9001))->toThrow(InvalidArgumentException::class)
            ->and(fn() => new WorkerCapacity(257, 256))->toThrow(InvalidArgumentException::class)
            ->and(fn() => new WorkerDescriptor(
                id: '',
                address: new WorkerAddress('127.0.0.1', 9001),
                capacity: new WorkerCapacity(0, 256),
                state: WorkerState::STARTING,
            ))->toThrow(InvalidArgumentException::class)
            ->and(fn() => new WorkerMessageTarget())->toThrow(InvalidArgumentException::class)
            ->and(fn() => WorkerMessageTarget::worker(''))->toThrow(InvalidArgumentException::class);
    });
});
