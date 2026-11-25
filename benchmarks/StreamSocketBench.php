<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocket;
use PhpBench\Attributes as Bench;

/** Real nonblocking stream operations, isolated from WebSocket framing and TCP setup. */
#[Bench\Groups(['checkpoint-runtime'])]
#[Bench\BeforeMethods('setUp')]
#[Bench\AfterMethods('tearDown')]
final class StreamSocketBench
{
    private StreamSocket $left;
    private StreamSocket $right;
    private string $payload;

    public function setUp(): void
    {
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($pair === false) {
            throw new \RuntimeException('Unable to create benchmark stream pair.');
        }
        try {
            $this->left = new StreamSocket($pair[0], 'local-left');
            $this->right = new StreamSocket($pair[1], 'local-right');
        } catch (\Throwable $error) {
            foreach ($pair as $resource) {
                if (is_resource($resource)) {
                    fclose($resource);
                }
            }
            throw $error;
        }
        $this->payload = str_repeat("\x00\xff\x42\xab", 16);
    }

    #[Bench\Revs(10_000)]
    public function benchWriteRead64Bytes(): void
    {
        $result = $this->left->write($this->payload);
        if ($result->status !== SocketWriteStatus::WRITTEN || $result->writtenBytes !== 64) {
            throw new \RuntimeException('Unexpected benchmark write result.');
        }
        $result = $this->right->read(64);
        if ($result->status !== SocketReadStatus::DATA || $result->bytes !== $this->payload) {
            throw new \RuntimeException('Unexpected benchmark read result.');
        }
    }

    #[Bench\Revs(10_000)]
    public function benchEmptyNonblockingRead(): void
    {
        $result = $this->right->read(64);
        if ($result->status !== SocketReadStatus::EMPTY || $result->bytes !== '') {
            throw new \RuntimeException('Unexpected benchmark empty read result.');
        }
    }

    public function tearDown(): void
    {
        $this->left->close();
        $this->right->close();
    }
}
