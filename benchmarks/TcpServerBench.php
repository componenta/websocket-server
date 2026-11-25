<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use PhpBench\Attributes as Bench;

/** Real loopback TCP workload; the listening process is started outside measurement. */
#[Bench\Groups(['network'])]
#[Bench\AfterMethods('tearDown')]
final class TcpServerBench
{
    /** @var resource|null */
    private mixed $process = null;
    /** @var list<resource> */
    private array $clients = [];
    private ?string $log = null;
    private string $address;
    private string $clientFrame;
    private string $expectedEcho;

    public function setUpServer(): void
    {
        $log = tempnam(sys_get_temp_dir(), 'ws-bench-');
        if ($log === false) {
            throw new \RuntimeException('Cannot create server log.');
        }
        $this->log = $log;
        $payload = str_repeat('x', 64);
        $this->clientFrame = "\x81\xc0mask" . ($payload ^ str_repeat('mask', 16));
        $this->expectedEcho = "\x81\x40" . $payload;

        try {
            $this->process = proc_open(
                [PHP_BINARY, '-d', 'opcache.enable_cli=0', '-d', 'opcache.jit=0', __DIR__ . '/Support/echo-server.php'],
                [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['file', $log, 'a']],
                $pipes,
            );

            if (!is_resource($this->process)) {
                throw new \RuntimeException('Cannot start benchmark server.');
            }
            fclose($pipes[0]);

            try {
                $read = [$pipes[1]];
                $write = $except = null;
                if (stream_select($read, $write, $except, 5) !== 1) {
                    throw new \RuntimeException('Benchmark server did not report readiness.');
                }
                $line = fgets($pipes[1]);
                if (!is_string($line) || preg_match('/^READY (127\.0\.0\.1:\d+)\r?\n$/D', $line, $matches) !== 1) {
                    throw new \RuntimeException('Unexpected benchmark readiness response.');
                }

                $this->address = $matches[1];
            } finally {
                fclose($pipes[1]);
            }
        } catch (\Throwable $error) {
            $details = (string) file_get_contents($log);
            $this->stopServer();
            throw new \RuntimeException($error->getMessage() . "\n" . $details, previous: $error);
        }
    }

    public function setUpClients(): void
    {
        $this->setUpServer();
        try {
            for ($i = 0; $i < 32; $i++) {
                $this->clients[] = $this->connect();
            }
        } catch (\Throwable $error) {
            $this->tearDown();
            throw $error;
        }
    }

    #[Bench\BeforeMethods('setUpClients')]
    #[Bench\Revs(2)]
    public function benchEcho512MessagesAcross32Connections(): void
    {
        for ($round = 0; $round < 16; $round++) {
            foreach ($this->clients as $client) {
                $this->writeAll($client, $this->clientFrame);
            }

            foreach ($this->clients as $client) {
                if ($this->readExact($client, strlen($this->expectedEcho)) !== $this->expectedEcho) {
                    throw new \RuntimeException('Invalid server echo.');
                }
            }
        }
    }

    #[Bench\BeforeMethods('setUpServer')]
    #[Bench\Revs(3)]
    public function benchUpgradeAndClose32Connections(): void
    {
        for ($i = 0; $i < 32; $i++) {
            $client = $this->connect();
            try {
                $this->closeHandshake($client);
            } finally {
                fclose($client);
            }
        }
    }

    public function tearDown(): void
    {
        try {
            foreach ($this->clients as $client) {
                try {
                    $this->closeHandshake($client);
                } finally {
                    fclose($client);
                }
            }
            $this->clients = [];
        } finally {
            // Also clean up all remaining clients if a closing handshake fails.
            foreach ($this->clients as $client) {
                if (is_resource($client)) {
                    fclose($client);
                }
            }
            $this->clients = [];
            $this->stopServer();
        }
    }

    /** @return resource */
    private function connect(): mixed
    {
        $client = stream_socket_client('tcp://' . $this->address, $errno, $error, 5);
        if (!is_resource($client)) {
            throw new \RuntimeException("Cannot connect: {$error} ({$errno})");
        }

        try {
            stream_set_timeout($client, 5);
            $request = "GET /ws HTTP/1.1\r\nHost: {$this->address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
                . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
            $this->writeAll($client, $request);
            $headers = '';
            while (!str_ends_with($headers, "\r\n\r\n")) {
                $line = fgets($client, 8192);
                if ($line === false || strlen($headers) + strlen($line) > 8192) {
                    throw new \RuntimeException('Missing or excessive handshake response.');
                }
                $headers .= $line;
            }

            if (!str_starts_with($headers, "HTTP/1.1 101 ")
                || preg_match('/\r\n(?i:sec-websocket-accept):[ \t]*s3pPLMBiTxaQ9kYGzzhZRbK\+xOo=\r\n/', $headers) !== 1
            ) {
                throw new \RuntimeException('Unexpected handshake response.');
            }

            return $client;
        } catch (\Throwable $error) {
            fclose($client);
            throw $error;
        }
    }

    /** @param resource $client */
    private function closeHandshake(mixed $client): void
    {
        $this->writeAll($client, "\x88\x82mask" . ("\x03\xe8" ^ 'ma'));
        if ($this->readExact($client, 4) !== "\x88\x02\x03\xe8") {
            throw new \RuntimeException('Invalid close response.');
        }
    }

    /** @param resource $client */
    private function writeAll(mixed $client, string $bytes): void
    {
        while ($bytes !== '') {
            $written = fwrite($client, $bytes);
            if ($written === false || $written === 0) {
                throw new \RuntimeException('Socket write failed or timed out.');
            }
            $bytes = substr($bytes, $written);
        }
    }

    /** @param resource $client */
    private function readExact(mixed $client, int $length): string
    {
        $bytes = '';
        while (strlen($bytes) < $length) {
            $part = fread($client, $length - strlen($bytes));
            if ($part === false || $part === '') {
                throw new \RuntimeException('Socket read failed, closed or timed out.');
            }
            $bytes .= $part;
        }

        return $bytes;
    }

    private function stopServer(): void
    {
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            $deadline = hrtime(true) + 2_000_000_000;
            while (proc_get_status($this->process)['running'] && hrtime(true) < $deadline) {
                usleep(1_000);
            }
            if (proc_get_status($this->process)['running']) {
                proc_terminate($this->process, 9);
            }
            proc_close($this->process);
            $this->process = null;
        }
        if ($this->log !== null && is_file($this->log)) {
            unlink($this->log);
            $this->log = null;
        }
    }
}
