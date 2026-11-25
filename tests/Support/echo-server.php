<?php

declare(strict_types=1);

use Componenta\WebSocket\Application\CallableMessageRouter;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextFactory;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\RoutedWebSocketApplication;
use Componenta\WebSocket\Application\SafeWebSocketApplicationInvoker;
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Config\WebSocketOptions;
use Componenta\WebSocket\Config\WebSocketOptionsInterface;
use Componenta\WebSocket\Connection\ConnectionFactory;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Connection\PendingConnectionFactory;
use Componenta\WebSocket\Loop\MonotonicClock;
use Componenta\WebSocket\Loop\SelectEventLoop;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Protocol\MessageAssemblerFactory;
use Componenta\WebSocket\Socket\SocketListenerFactoryInterface;
use Componenta\WebSocket\Socket\SocketListenerInterface;
use Componenta\WebSocket\Socket\StreamSocketListener;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Transport\Server;

$loader = require dirname(__DIR__, 2) . '/vendor/autoload.php';

// Network benchmarks may compare runtime-compatible revisions. Stress tests
// deliberately use the checked-out source unless a benchmark source is set.
$source = getenv('WEBSOCKET_BENCH_SOURCE');
if ($source !== false && $source !== '') {
    $source = realpath($source);
    if ($source === false || !is_file($source . '/Transport/Server.php')) {
        throw new RuntimeException('Invalid benchmark server source directory.');
    }
    $loader->setPsr4('Componenta\\WebSocket\\', [$source]);
    $file = (new ReflectionClass(Server::class))->getFileName();
    if (!is_string($file) || !str_starts_with($file, $source . DIRECTORY_SEPARATOR)) {
        throw new RuntimeException('Benchmark server loaded the wrong revision.');
    }
}

// Bind an ephemeral loopback port without a release/rebind race.
$listenerFactory = new class implements SocketListenerFactoryInterface {
    public function listen(WebSocketOptionsInterface $options): SocketListenerInterface
    {
        $resource = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        if (!is_resource($resource)) {
            throw new RuntimeException("Unable to bind benchmark listener: {$error} ({$errno})");
        }

        $address = stream_socket_get_name($resource, false);
        if (!is_string($address)) {
            fclose($resource);
            throw new RuntimeException('Cannot determine benchmark listener address.');
        }

        $listener = new StreamSocketListener($resource);
        fwrite(STDOUT, "READY {$address}\n");
        fflush(STDOUT);

        return $listener;
    }
};

$options = new WebSocketOptions(heartbeatIntervalMs: 0, shutdownDrainTimeoutMs: 100);
$clock = new MonotonicClock();
$errors = new WebSocketErrorContextFactory();

$server = new Server(
    $options,
    $listenerFactory,
    new SelectEventLoop(new StreamSocketSelector(), $clock, $options->selectTimeoutUsec),
    new Handshake($options),
    new ConnectionFactory(new FrameCodec(), $options, new MessageAssemblerFactory()),
    new PendingConnectionFactory($clock),
    new SafeWebSocketApplicationInvoker($errors),
    $errors,
    $clock,
);

$registry = new InMemoryConnectionRegistry();
$application = new RoutedWebSocketApplication(
    new CallableMessageRouter(static function (ConnectionInterface $connection, Message $message): void {
        $result = $connection->send($message);
        if (!$result->accepted) {
            throw new RuntimeException($result->reason ?? 'Echo rejected.');
        }
    }),
    $registry,
);

// Keep the measured PHPBench path uninstrumented. The stress test opts in to
// exact callback accounting and verifies all sessions leave the registry.
$statsFile = getenv('WEBSOCKET_STRESS_STATS');
$instrumented = null;
if ($statsFile !== false && $statsFile !== '') {
    if (!function_exists('pcntl_async_signals') || !function_exists('pcntl_signal')) {
        throw new RuntimeException('The stress fixture requires ext-pcntl.');
    }
    $instrumented = new class($application, $registry) implements WebSocketApplicationInterface {
        private int $connected = 0;
        private int $disconnected = 0;
        private int $clean = 0;
        private int $abnormal = 0;
        private int $received = 0;
        private int $failures = 0;

        public function __construct(
            private readonly WebSocketApplicationInterface $delegate,
            private readonly InMemoryConnectionRegistry $registry,
        ) {}

        public function connected(ConnectionInterface $connection): void
        {
            $this->delegate->connected($connection);
            $this->connected++;
        }

        public function received(ConnectionInterface $connection, Message $message): void
        {
            $this->delegate->received($connection, $message);
            $this->received++;
        }

        public function disconnected(ConnectionInterface $connection, CloseInfo $close): void
        {
            $this->delegate->disconnected($connection, $close);
            $this->disconnected++;
            if ($close->clean) {
                $this->clean++;
            } else {
                $this->abnormal++;
            }
        }

        public function failed(WebSocketErrorContextInterface $context): void
        {
            $this->failures++;
            fwrite(STDERR, $context->phase->value . ': ' . $context->error->getMessage() . PHP_EOL);
        }

        public function stats(): array
        {
            return [
                'connected' => $this->connected,
                'disconnected' => $this->disconnected,
                'clean' => $this->clean,
                'abnormal' => $this->abnormal,
                'received' => $this->received,
                'failures' => $this->failures,
                'registry_count' => $this->registry->count(),
                'php_used_bytes' => memory_get_usage(),
                'php_peak_bytes' => memory_get_peak_usage(),
            ];
        }
    };
    $application = $instrumented;
}

if (function_exists('pcntl_async_signals') && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static fn () => $server->stop());
}

$server->run($application);

if ($instrumented !== null) {
    if (file_put_contents($statsFile, json_encode($instrumented->stats(), JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)) === false) {
        throw new RuntimeException('Unable to write stress statistics.');
    }
}
