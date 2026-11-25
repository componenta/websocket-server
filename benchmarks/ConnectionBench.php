<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Benchmarks\Support\ConnectionFixture;
use Componenta\WebSocket\Benchmarks\Support\CountingSocket;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\Opcode;
use PhpBench\Attributes as Bench;

#[Bench\Groups(['comparative'])]
#[Bench\BeforeMethods('setUp')]
#[Bench\AfterMethods('tearDown')]
final class ConnectionBench
{
    private ConnectionFixture $fixture;
    private CountingSocket $socket;
    private Connection $connection;
    private \Closure $echo;
    private \Closure $receive;
    private string $wire;
    private int $received = 0;

    public function setUp(): void
    {
        $this->fixture = new ConnectionFixture();
        $this->socket = new CountingSocket();
        $this->connection = $this->fixture->create($this->socket);
        $payload = str_repeat('x', 64);
        $this->wire = (new FrameCodec())->encode(new Frame(Opcode::TEXT, $payload), true, 'mask');

        $handle = method_exists($this->connection, 'receiveBytes') ? $this->connection->application : $this->connection;
        $this->echo = function (Message $message) use ($handle, $payload): void {
            if ($message->payload !== $payload || !$handle->send($message)->accepted) {
                throw new \RuntimeException('Echo failed.');
            }
            $this->received++;
        };

        if (method_exists($this->connection, 'receiveBytes')) {
            $this->connection->setWriteNotifier(static function (): void {});
            $this->receive = function (): void {
                $this->connection->receiveBytes($this->wire, $this->echo);
            };
        } else {
            $this->receive = function (): void {
                foreach ($this->connection->appendBytes($this->wire) as $message) {
                    ($this->echo)($message);
                }
            };
        }
    }

    public function tearDown(): void
    {
        $this->connection->markClosed();
    }

    #[Bench\Revs(20_000)]
    public function benchSmallEcho(): void
    {
        $before = $this->socket->written;
        $received = $this->received;
        ($this->receive)();
        if (!$this->connection->flush() || $this->connection->needsWrite
            || $this->socket->written - $before !== 66 || $this->received !== $received + 1
        ) {
            throw new \RuntimeException('Small echo did not drain one message.');
        }
    }

    #[Bench\Revs(100)]
    public function benchQueueAndDrain100SmallMessages(): void
    {
        $before = $this->socket->written;
        for ($i = 0; $i < 100; $i++) {
            ($this->receive)();
        }

        $this->drain($this->connection, 100);
        if ($this->socket->written - $before !== 6_600) {
            throw new \RuntimeException('Incorrect batch byte count.');
        }
    }

    private function drain(Connection $connection, int $limit): void
    {
        for ($i = 0; $connection->needsWrite && $i < $limit; $i++) {
            if (!$connection->flush()) {
                throw new \RuntimeException('Write failed.');
            }
        }

        if ($connection->needsWrite) {
            throw new \RuntimeException('Queue did not drain.');
        }
    }
}
