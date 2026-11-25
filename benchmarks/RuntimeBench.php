<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Benchmarks\Support\ConnectionFixture;
use Componenta\WebSocket\Benchmarks\Support\CountingSocket;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Connection\PendingConnection;
use Componenta\WebSocket\Loop\MonotonicClock;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use PhpBench\Attributes as Bench;

/** Workloads specific to the current runtime contract, without a legacy timing baseline. */
#[Bench\Groups(['runtime'])]
final class RuntimeBench
{
    private SelectEventLoop $loop;
    private ConnectionFixture $fixture;
    private CountingSocket $socket;
    private Connection $connection;
    private string $request;
    private string $response;

    public function setUpLoop(): void
    {
        $this->loop = new SelectEventLoop(new StreamSocketSelector(), new MonotonicClock(), 0);
    }

    public function setUpConnection(): void
    {
        $this->fixture = new ConnectionFixture();
        $this->socket = new CountingSocket();
        $this->connection = $this->fixture->create($this->socket);
    }

    public function tearDownConnection(): void
    {
        $this->connection->markClosed();
    }

    public function setUpHandshake(): void
    {
        $this->request = "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
            . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
        $this->response = (new Handshake(new WebSocketOptions()))->accept($this->request)->response;
    }

    #[Bench\BeforeMethods('setUpLoop')]
    #[Bench\Revs(100)]
    public function benchDispatch100DeferredCallbacks(): void
    {
        $count = 0;
        $callback = static function () use (&$count): void {
            $count++;
        };

        for ($i = 0; $i < 100; $i++) {
            $this->loop->defer($callback);
        }

        $this->loop->run();
        if ($count !== 100) {
            throw new \RuntimeException('Deferred callbacks were lost.');
        }
    }

    #[Bench\BeforeMethods('setUpLoop')]
    #[Bench\Revs(100)]
    public function benchRegisterAndCancel100Timers(): void
    {
        $timers = [];
        $callback = static function (): void {
            throw new \RuntimeException('Cancelled timer was invoked.');
        };

        for ($i = 0; $i < 100; $i++) {
            $timers[] = $this->loop->delay(10_000, $callback);
        }
        foreach ($timers as $timer) {
            $timer->cancel();
            if ($timer->enabled) {
                throw new \RuntimeException('Timer was not cancelled.');
            }
        }
    }

    #[Bench\BeforeMethods('setUpHandshake')]
    #[Bench\Revs(2_000)]
    public function benchPendingHandshakeAndPartialResponse(): void
    {
        $socket = new CountingSocket(16);
        $pending = new PendingConnection($socket, 0.0);
        $pending->append(substr($this->request, 0, 32));
        $pending->append(substr($this->request, 32));
        if (!$pending->ready) {
            throw new \RuntimeException('Handshake not assembled.');
        }

        $pending->queueResponse($this->response);
        for ($i = 0; $pending->needsWrite && $i < 100; $i++) {
            if (!$pending->flush()) {
                throw new \RuntimeException('Handshake write failed.');
            }
        }
        if ($pending->needsWrite || $socket->written !== strlen($this->response)) {
            throw new \RuntimeException('Handshake response not drained.');
        }
    }

    #[Bench\BeforeMethods('setUpConnection')]
    #[Bench\AfterMethods('tearDownConnection')]
    #[Bench\Revs(5_000)]
    public function benchMatchingPingPong(): void
    {
        if (!$this->connection->ping('ping')->accepted || !$this->connection->flush()) {
            throw new \RuntimeException('Ping not sent.');
        }
        $this->connection->receiveBytes("\x8a\x84mask" . ('ping' ^ 'mask'), self::unexpectedMessage(...));

        if ($this->connection->awaitingPong) {
            throw new \RuntimeException('Matching Pong not recognized.');
        }
    }

    #[Bench\BeforeMethods('setUpConnection')]
    #[Bench\AfterMethods('tearDownConnection')]
    #[Bench\Revs(2_000)]
    public function benchCompleteClosingHandshake(): void
    {
        $connection = $this->fixture->create(new CountingSocket());
        try {
            if (!$connection->close()->accepted || !$connection->flush() || $connection->readyToClose) {
                throw new \RuntimeException('Incorrect local close.');
            }

            $connection->receiveBytes("\x88\x82mask" . (pack('n', CloseCode::NORMAL) ^ 'ma'), self::unexpectedMessage(...));
            if (!$connection->readyToClose) {
                throw new \RuntimeException('Missing peer close.');
            }
        } finally {
            $connection->markClosed();
        }
        if ($connection->closeInfo?->clean !== true) {
            throw new \RuntimeException('Incomplete closing handshake.');
        }
    }

    private static function unexpectedMessage(Message $message): void
    {
        throw new \RuntimeException('Control frame leaked into application messages.');
    }
}
