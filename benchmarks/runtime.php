<?php

declare(strict_types=1);

use Componenta\WebSocket\Connection\BufferBudget;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;

require $argv[1] ?? dirname(__DIR__) . '/vendor/autoload.php';

final class BenchmarkSocket implements SocketInterface
{
    public readonly string $id;
    public readonly string $remoteAddress;
    public bool $eof { get => false; }
    public int $written = 0;

    public function __construct(private readonly int $writeLimit = PHP_INT_MAX)
    {
        $this->id = 'benchmark';
        $this->remoteAddress = '127.0.0.1';
    }

    public function read(int $maxBytes): SocketReadResult
    {
        return new SocketReadResult(SocketReadStatus::EMPTY);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $count = min(strlen($bytes), $this->writeLimit);
        $this->written += $count;
        return new SocketWriteResult(SocketWriteStatus::WRITTEN, $count);
    }

    public function close(): void {}
}

/** Uses shared budgets when testing a version that supports them. */
function benchmarkConnection(BenchmarkSocket $socket, FrameCodec $codec): Connection
{
    $arguments = [
        'socket' => $socket,
        'codec' => $codec,
        'assembler' => new MessageAssembler(1_048_576),
        'maxPayloadSize' => 1_048_576,
        'id' => 'benchmark',
        'request' => new HandshakeRequest('GET', '/ws', '/ws', []),
    ];

    if (class_exists(BufferBudget::class)) {
        $arguments['incomingBudget'] = new BufferBudget(67_108_864);
        $arguments['outgoingBudget'] = new BufferBudget(67_108_864);
    }

    return new Connection(...$arguments);
}

function ensure(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

/**
 * Measure a complete operation with correctness checks enabled.
 *
 * @return array{median_ms: float, min_ms: float, max_ms: float, peak_extra_bytes: int, samples_ms: list<float>}
 */
function measure(Closure $operation): array
{
    $operation();
    $times = [];
    $peak = 0;

    for ($sample = 0; $sample < 5; $sample++) {
        gc_collect_cycles();
        memory_reset_peak_usage();
        $memory = memory_get_usage();
        $start = hrtime(true);
        $operation();
        $times[] = (hrtime(true) - $start) / 1_000_000;
        $peak = max($peak, memory_get_peak_usage() - $memory);
    }

    sort($times, SORT_NUMERIC);
    return [
        'median_ms' => round($times[2], 3),
        'min_ms' => round($times[0], 3),
        'max_ms' => round($times[4], 3),
        'peak_extra_bytes' => $peak,
        'samples_ms' => array_map(static fn (float $time): float => round($time, 3), $times),
    ];
}

$codec = new FrameCodec();
$results = [];

foreach (['small' => [64, 20_000], 'large' => [1_048_576, 30]] as $name => [$size, $iterations]) {
    $payload = substr(str_repeat("\x01\xfe\x42\xab", intdiv($size + 3, 4)), 0, $size);
    $frame = new Frame(Opcode::BINARY, $payload);

    $results["masked_encode_decode_{$name}"] = measure(static function () use ($codec, $frame, $iterations, $payload): void {
        for ($i = 0; $i < $iterations; $i++) {
            $wire = $codec->encode($frame, true, 'mask');
            $decoded = $codec->decode($wire);
            ensure($decoded?->opcode === Opcode::BINARY && $decoded->payload === $payload && $wire === '', 'Frame round trip failed.');
        }
    });
}

$payload = str_repeat('x', 64);
$wire = $codec->encode(new Frame(Opcode::TEXT, $payload), true, 'mask');
$results['small_message_echo_20000'] = measure(static function () use ($codec, $wire, $payload): void {
    $socket = new BenchmarkSocket();
    $connection = benchmarkConnection($socket, $codec);
    $received = 0;
    $handler = static function (Message $message) use ($connection, &$received, $payload): void {
        ensure($message->payload === $payload, 'Incorrect received payload.');
        ensure($connection->send($message)->accepted, 'Echo rejected.');
        $received++;
    };

    if (method_exists($connection, 'receiveBytes')) {
        for ($i = 0; $i < 20_000; $i++) {
            $connection->receiveBytes($wire, $handler);
            ensure($connection->flush(), 'Echo write failed.');
        }
    } else {
        for ($i = 0; $i < 20_000; $i++) {
            foreach ($connection->appendBytes($wire) as $message) {
                $handler($message);
            }
            ensure($connection->flush(), 'Echo write failed.');
        }
    }

    ensure($received === 20_000 && $connection->queuedBytes === 0 && $socket->written === 20_000 * 66, 'Echo byte count mismatch.');
    $connection->markClosed();
});

$payload = str_repeat('x', 2_097_152);
$results['partial_write_2mib_1kib_chunks'] = measure(static function () use ($codec, $payload): void {
    $socket = new BenchmarkSocket(1024);
    $connection = benchmarkConnection($socket, $codec);
    ensure($connection->sendBinary($payload)->accepted, 'Large outgoing frame rejected.');

    $flushes = 0;
    while ($connection->needsWrite && $flushes++ < 3_000) {
        ensure($connection->flush(), 'Partial write failed.');
    }

    ensure(!$connection->needsWrite && $socket->written === strlen($payload) + 10, 'Partial write byte count mismatch.');
    $connection->markClosed();
});

echo json_encode([
    'php' => PHP_VERSION,
    'architecture' => php_uname('m'),
    'jit' => ini_get('opcache.jit'),
    'benchmarks' => $results,
], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT) . PHP_EOL;
