<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Benchmarks\Support\ConnectionFixture;
use Componenta\WebSocket\Benchmarks\Support\CountingSocket;
use PhpBench\Attributes as Bench;

#[Bench\Groups(['comparative'])]
final class ConnectionLifecycleBench
{
    private ConnectionFixture $fixture;
    private string $large;

    public function setUp(): void
    {
        $this->fixture = new ConnectionFixture();
    }

    public function setUpLarge(): void
    {
        $this->setUp();
        $this->large = str_repeat('x', 2_097_152);
    }

    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(5_000)]
    public function benchConnectionLifecycle(): void
    {
        $socket = new CountingSocket();
        $connection = $this->fixture->create($socket);

        try {
            if (!$connection->sendText('hello')->accepted || !$connection->flush()
                || $socket->written !== 7 || !$connection->close()->accepted
                || !$connection->flush() || $socket->written !== 11
            ) {
                throw new \RuntimeException('Connection lifecycle failed.');
            }
        } finally {
            $connection->markClosed();
        }
        if (!$connection->closed || $connection->needsWrite) {
            throw new \RuntimeException('Connection resources were not released.');
        }
    }

    #[Bench\BeforeMethods('setUpLarge')]
    #[Bench\Revs(3)]
    public function benchPartialWrite2MiB(): void
    {
        $socket = new CountingSocket(1_024);
        $connection = $this->fixture->create($socket);

        try {
            if (!$connection->sendBinary($this->large)->accepted) {
                throw new \RuntimeException('Large frame rejected.');
            }

            for ($i = 0; $connection->needsWrite && $i < 3_000; $i++) {
                if (!$connection->flush()) {
                    throw new \RuntimeException('Write failed.');
                }
            }

            if ($connection->needsWrite || $socket->written !== 2_097_162) {
                throw new \RuntimeException('Partial write byte count mismatch.');
            }
        } finally {
            $connection->markClosed();
        }
    }
}
