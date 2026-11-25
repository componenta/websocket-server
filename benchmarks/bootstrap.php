<?php

declare(strict_types=1);

use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\Handshake;

$loader = require dirname(__DIR__) . '/vendor/autoload.php';

// Compare two package revisions against the same installed dependencies.
$source = getenv('WEBSOCKET_BENCH_SOURCE');
$source = realpath($source === false || $source === '' ? dirname(__DIR__) . '/src' : $source);
if ($source === false || !is_file($source . '/Protocol/FrameCodec.php')) {
    throw new RuntimeException('Invalid WebSocket benchmark source directory.');
}

$loader->setPsr4('Componenta\\WebSocket\\', [$source]);

foreach ([Connection::class, FrameCodec::class, Handshake::class] as $class) {
    $file = (new ReflectionClass($class))->getFileName();
    if (!is_string($file) || !str_starts_with($file, $source . DIRECTORY_SEPARATOR)) {
        throw new RuntimeException('Benchmark loaded the wrong package revision: ' . $class);
    }
}

require_once __DIR__ . '/Support/CountingSocket.php';
require_once __DIR__ . '/Support/ConnectionFixture.php';
