<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Opcode;

function websocketE2EFreePort(): int
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);

    if (!is_resource($server)) {
        throw new RuntimeException(sprintf('Unable to allocate TCP port: [%d] %s', $errno, $error));
    }

    $name = stream_socket_get_name($server, false);
    fclose($server);

    if (!is_string($name) || !str_contains($name, ':')) {
        throw new RuntimeException('Unable to determine allocated TCP port.');
    }

    return (int) substr(strrchr($name, ':'), 1);
}

function websocketE2EReadUntil(mixed $stream, string $needle, float $timeoutSeconds = 5.0): string
{
    $buffer = '';
    $deadline = microtime(true) + $timeoutSeconds;

    while (!str_contains($buffer, $needle) && microtime(true) < $deadline) {
        $chunk = fread($stream, 8192);

        if (is_string($chunk) && $chunk !== '') {
            $buffer .= $chunk;
            continue;
        }

        usleep(10_000);
    }

    return $buffer;
}

function websocketE2EReadFrames(mixed $stream, int $expectedFrames, float $timeoutSeconds = 5.0): array
{
    $codec = new FrameCodec();
    $buffer = '';
    $frames = [];
    $deadline = microtime(true) + $timeoutSeconds;

    while (count($frames) < $expectedFrames && microtime(true) < $deadline) {
        $chunk = fread($stream, 8192);

        if (is_string($chunk) && $chunk !== '') {
            $buffer .= $chunk;

            while (($frame = $codec->decode($buffer, expectMasked: false)) !== null) {
                $frames[] = $frame;
            }

            continue;
        }

        usleep(10_000);
    }

    return $frames;
}

function websocketE2EServerScript(string $workspace): string
{
    $autoload = str_replace('\\', '/', $workspace . '/vendor/autoload.php');

    return <<<PHP
<?php

declare(strict_types=1);

require '{$autoload}';

use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Transport\Server;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\StreamSocketListenerFactory;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Transport\WebSocketServerInterface;
use Psr\Clock\ClockInterface;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\Frame;

final readonly class E2EReadyListenerFactory implements SocketListenerFactoryInterface
{
    public function __construct(
        private SocketListenerFactoryInterface \$listeners,
    ) {}

    public function listen(WebSocketOptionsInterface \$options): SocketListenerInterface
    {
        \$listener = \$this->listeners->listen(\$options);
        fwrite(STDOUT, "READY\n");
        fflush(STDOUT);

        return \$listener;
    }
}

final readonly class E2EClock implements ClockInterface
{
    public function now(): DateTimeImmutable
    {
        return new DateTimeImmutable();
    }
}

final class E2EApplication implements WebSocketApplicationInterface
{
    public ?WebSocketServerInterface \$server = null;

    public function connected(ConnectionInterface \$connection): void {}

    public function received(ConnectionInterface \$connection, Message \$message): void
    {
        \$connection->sendText('echo:' . \$message->payload);
        \$connection->close();
    }

    public function disconnected(ConnectionInterface \$connection, CloseInfo \$close): void
    {
        \$this->server?->stop();
    }

    public function failed(WebSocketErrorContextInterface \$context): void
    {
        fwrite(STDERR, \$context->phase->value . ':' . \$context->error->getMessage() . "\n");
        fflush(STDERR);
        \$this->server?->stop();
    }
}

\$port = (int) (\$argv[1] ?? 0);
\$options = new WebSocketOptions(
    host: '127.0.0.1',
    port: \$port,
    path: '/ws',
    heartbeatIntervalMs: 0,
    pongTimeoutMs: 1000,
    idleTimeoutMs: 0,
    selectTimeoutUsec: 10_000,
    shutdownDrainTimeoutMs: 1000,
);
\$clock = new E2EClock();
\$server = new Server(
    \$options,
    new E2EReadyListenerFactory(new StreamSocketListenerFactory()),
    new SelectEventLoop(new StreamSocketSelector(), \$clock, \$options->selectTimeoutUsec),
    new Handshake(\$options),
    new ConnectionFactory(new FrameCodec(), \$options, new MessageAssemblerFactory()),
    new PendingConnectionFactory(\$clock),
    new SafeWebSocketApplicationInvoker(new WebSocketErrorContextFactory()),
    new WebSocketErrorContextFactory(),
    \$clock,
);
\$application = new E2EApplication();
\$application->server = \$server;
\$server->run(\$application);
PHP;
}

describe('WebSocket server e2e', function () {
    it('accepts a real TCP WebSocket client, echoes a message, and closes cleanly', function () {
        if (!function_exists('proc_open')) {
            $this->markTestSkipped('proc_open is required for the WebSocket e2e server process.');
        }

        $port = websocketE2EFreePort();
        $script = tempnam(sys_get_temp_dir(), 'componenta-ws-e2e-');
        $process = null;
        $pipes = [];

        expect($script)->toBeString();
        file_put_contents($script, websocketE2EServerScript((string) getcwd()));

        try {
            $process = proc_open(
                escapeshellarg(PHP_BINARY) . ' ' . escapeshellarg($script) . ' ' . $port,
                [
                    0 => ['pipe', 'r'],
                    1 => ['pipe', 'w'],
                    2 => ['pipe', 'w'],
                ],
                $pipes,
                (string) getcwd(),
            );

            expect(is_resource($process))->toBeTrue();
            fclose($pipes[0]);
            stream_set_blocking($pipes[1], false);
            stream_set_blocking($pipes[2], false);

            $ready = false;
            $stderr = '';
            $deadline = microtime(true) + 5;

            while (!$ready && microtime(true) < $deadline) {
                $line = fgets($pipes[1]);

                if (is_string($line) && trim($line) === 'READY') {
                    $ready = true;
                    break;
                }

                $error = stream_get_contents($pipes[2]);
                $stderr .= is_string($error) ? $error : '';
                usleep(10_000);
            }

            expect($ready)->toBeTrue("WebSocket e2e server did not become ready. {$stderr}");

            $client = stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $error, 5);
            expect(is_resource($client))->toBeTrue("Unable to connect to e2e WebSocket server: [{$errno}] {$error}");
            stream_set_timeout($client, 2);

            $key = base64_encode(random_bytes(16));
            fwrite(
                $client,
                "GET /ws HTTP/1.1\r\n"
                . "Host: 127.0.0.1:{$port}\r\n"
                . "Connection: Upgrade\r\n"
                . "Upgrade: websocket\r\n"
                . "Sec-WebSocket-Version: 13\r\n"
                . "Sec-WebSocket-Key: {$key}\r\n"
                . "\r\n",
            );

            $response = websocketE2EReadUntil($client, "\r\n\r\n");
            $accept = base64_encode(sha1($key . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', true));

            expect($response)->toContain('HTTP/1.1 101 Switching Protocols')
                ->and($response)->toContain("Sec-WebSocket-Accept: {$accept}");

            fwrite($client, (new FrameCodec())->encode(
                new Componenta\WebSocket\Protocol\Frame(Opcode::TEXT, 'hello'),
                masked: true,
                maskKey: 'mask',
            ));

            $frames = websocketE2EReadFrames($client, 2);
            fclose($client);

            expect($frames)->toHaveCount(2)
                ->and($frames[0]->opcode)->toBe(Opcode::TEXT)
                ->and($frames[0]->payload)->toBe('echo:hello')
                ->and($frames[1]->opcode)->toBe(Opcode::CLOSE);

            $deadline = microtime(true) + 5;

            do {
                $status = proc_get_status($process);

                if (!$status['running']) {
                    break;
                }

                usleep(10_000);
            } while (microtime(true) < $deadline);

            $status = proc_get_status($process);
            expect($status['running'])->toBeFalse('WebSocket e2e server process did not exit.');
        } finally {
            if (is_resource($process ?? null)) {
                $status = proc_get_status($process);

                if ($status['running']) {
                    proc_terminate($process);
                }

                foreach ($pipes as $pipe) {
                    if (is_resource($pipe)) {
                        fclose($pipe);
                    }
                }

                proc_close($process);
            }

            if (is_string($script) && is_file($script)) {
                unlink($script);
            }
        }
    });
});
