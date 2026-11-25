<?php

declare(strict_types=1);

use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocket;

/** Test stream whose wrapper refuses nonblocking mode. */
final class BlockingOnlyWebSocketTestStream
{
    public mixed $context = null;
    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool { return true; }
    public function stream_set_option(int $option, int $arg1, ?int $arg2): bool { return false; }
    public function stream_eof(): bool { return false; }
}

/** Stream-level I/O failures without network timing dependencies. */
final class FailingWebSocketTestStream
{
    public mixed $context = null;
    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool { return true; }
    public function stream_set_option(int $option, int $arg1, ?int $arg2): bool { return true; }
    public function stream_eof(): bool { return false; }
    public function stream_read(int $count): false { return false; }
    public function stream_write(string $bytes): false { return false; }
}

describe('Stream socket lifecycle', function () {
    it('keeps closed sockets terminal and reports typed read and write outcomes', function () {
        $resources = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        expect($resources)->not->toBeFalse();
        $socket = new StreamSocket($resources[0], 'local');
        $id = $socket->id;

        try {
            $socket->close();
            $socket->close();

            expect($socket->id)->toBe($id)
                ->and($socket->eof)->toBeTrue()
                ->and($socket->read(8192)->status)->toBe(SocketReadStatus::CLOSED)
                ->and($socket->write('late')->status)->toBe(SocketWriteStatus::ERROR);
        } finally {
            $socket->close();
            fclose($resources[1]);
        }
    });

    it('reads and writes an open stream without changing the status contract', function () {
        $resources = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        expect($resources)->not->toBeFalse();
        $socket = new StreamSocket($resources[0], 'local');

        try {
            expect($socket->read(8192)->status)->toBe(SocketReadStatus::EMPTY);
            fwrite($resources[1], 'hello');
            $read = $socket->read(8192);

            expect($read->status)->toBe(SocketReadStatus::DATA)
                ->and($read->bytes)->toBe('hello')
                ->and($socket->write('reply')->status)->toBe(SocketWriteStatus::WRITTEN)
                ->and(fread($resources[1], 5))->toBe('reply');
        } finally {
            $socket->close();
            fclose($resources[1]);
        }
    });

    it('does not accept connections or throw after a stream listener is closed', function () {
        $resource = stream_socket_server('tcp://127.0.0.1:0');
        expect($resource)->not->toBeFalse();
        $listener = new Componenta\WebSocket\Socket\StreamSocketListener($resource);
        $id = $listener->id;

        try {
            $listener->close();
            $listener->close();
            expect($listener->id)->toBe($id)
                ->and($listener->accept())->toBeNull();
        } finally {
            $listener->close();
        }
    });


    it('binds IPv6 listeners with either raw or bracketed host addresses', function (string $host) {
        $probe = @stream_socket_server('tcp://[::1]:0', $errno, $error);
        if (!is_resource($probe)) {
            $this->markTestSkipped('IPv6 loopback unavailable: ' . $error);
        }
        $address = stream_socket_get_name($probe, false);
        fclose($probe);
        expect($address)->toBeString();
        $port = (int) substr(strrchr($address, ':'), 1);
        $factory = new Componenta\WebSocket\Socket\StreamSocketListenerFactory();
        $listener = $factory->listen(new Componenta\WebSocket\Config\WebSocketOptions(host: $host, port: $port));
        $client = null;
        $accepted = null;

        try {
            $client = stream_socket_client('tcp://[::1]:' . $port, $errno, $error, 1);
            expect(is_resource($client))->toBeTrue();
            stream_set_timeout($client, 1);
            $selector = new Componenta\WebSocket\Socket\StreamSocketSelector();
            expect($selector->select([$listener], [], [], 1_000_000)->canReadListener($listener))->toBeTrue();
            $accepted = $listener->accept();
            expect($accepted)->toBeInstanceOf(Componenta\WebSocket\Socket\SocketInterface::class);
            expect($accepted->write('ipv6')->writtenBytes)->toBe(4)
                ->and(fread($client, 4))->toBe('ipv6');
        } finally {
            $accepted?->close();
            if (is_resource($client)) {
                fclose($client);
            }
            $listener->close();
        }
    })->with(['::1', '[::1]']);


    it('rejects stream resources that refuse nonblocking mode', function (bool $listener) {
        $scheme = 'wsblockingtest';
        expect(stream_wrapper_register($scheme, BlockingOnlyWebSocketTestStream::class))->toBeTrue();
        $resource = null;
        try {
            $resource = fopen($scheme . '://resource', 'r+');
            expect(is_resource($resource))->toBeTrue();
            expect(fn () => $listener
                ? new Componenta\WebSocket\Socket\StreamSocketListener($resource)
                : new StreamSocket($resource, 'local'))->toThrow(RuntimeException::class);
            // On constructor failure the caller retains ownership.
            expect(is_resource($resource))->toBeTrue();
        } finally {
            if (is_resource($resource)) {
                fclose($resource);
            }
            stream_wrapper_unregister($scheme);
        }
    })->with(['connection stream' => false, 'listener stream' => true]);

    it('does not allow cloning live socket resource owners', function () {
        $resources = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        expect($resources)->not->toBeFalse();
        $socket = new StreamSocket($resources[0], 'local');
        $resource = stream_socket_server('tcp://127.0.0.1:0');
        expect($resource)->not->toBeFalse();
        $listener = new Componenta\WebSocket\Socket\StreamSocketListener($resource);

        try {
            expect(fn () => clone $socket)->toThrow(Error::class);
            expect(fn () => clone $listener)->toThrow(Error::class);
            expect($socket->write('hello')->writtenBytes)->toBe(5)
                ->and(fread($resources[1], 5))->toBe('hello')
                ->and($listener->accept())->toBeNull();
        } finally {
            $socket->close();
            fclose($resources[1]);
            $listener->close();
        }
    });

    it('distinguishes an empty nonblocking read from a peer closing its stream', function () {
        $resources = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        expect($resources)->not->toBeFalse();
        $socket = new StreamSocket($resources[0], 'local');
        try {
            expect($socket->read(1)->status)->toBe(SocketReadStatus::EMPTY)
                ->and($socket->write('')->writtenBytes)->toBe(0);
            fwrite($resources[1], 'end');
            fclose($resources[1]);
            expect($socket->read(16)->bytes)->toBe('end')
                ->and($socket->read(16)->status)->toBe(SocketReadStatus::CLOSED)
                ->and($socket->eof)->toBeTrue();
        } finally {
            if (is_resource($resources[1])) {
                fclose($resources[1]);
            }
            $socket->close();
        }
    });

    it('reports a bind failure without closing a different listening socket', function () {
        $resource = stream_socket_server('tcp://127.0.0.1:0');
        expect($resource)->not->toBeFalse();
        $address = stream_socket_get_name($resource, false);
        expect($address)->toBeString();
        $port = (int) substr(strrchr($address, ':'), 1);
        try {
            $factory = new Componenta\WebSocket\Socket\StreamSocketListenerFactory();
            expect(fn () => $factory->listen(new Componenta\WebSocket\Config\WebSocketOptions(port: $port)))
                ->toThrow(RuntimeException::class);
            expect(is_resource($resource))->toBeTrue();
        } finally {
            fclose($resource);
        }
    });


    it('reports native stream read and write failures through their typed results', function () {
        $scheme = 'wsfailuretest';
        expect(stream_wrapper_register($scheme, FailingWebSocketTestStream::class))->toBeTrue();
        $resource = null;
        $socket = null;
        try {
            $resource = fopen($scheme . '://resource', 'r+');
            expect(is_resource($resource))->toBeTrue();
            $socket = new StreamSocket($resource, 'local');
            $read = $socket->read(32);
            $write = $socket->write('data');

            expect($read->status)->toBe(SocketReadStatus::ERROR)
                ->and($read->bytes)->toBe('')
                ->and($read->reason)->not->toBeNull()
                ->and($write->status)->toBe(SocketWriteStatus::ERROR)
                ->and($write->writtenBytes)->toBe(0)
                ->and($write->reason)->not->toBeNull();
        } finally {
            $socket?->close();
            if (is_resource($resource)) {
                fclose($resource);
            }
            stream_wrapper_unregister($scheme);
        }
    });

    it('reports real socket backpressure without blocking the writer', function () {
        $resources = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        expect($resources)->not->toBeFalse();
        $socket = new StreamSocket($resources[0], 'local');
        stream_set_blocking($resources[1], false);
        try {
            $payload = str_repeat('x', 65_536);
            $sent = 0;
            $blocked = false;
            for ($i = 0; $i < 1024; $i++) {
                $result = $socket->write($payload);
                if ($result->status === SocketWriteStatus::BLOCKED) {
                    $blocked = true;
                    expect($result->writtenBytes)->toBe(0);
                    break;
                }
                expect($result->status)->toBe(SocketWriteStatus::WRITTEN)
                    ->and($result->writtenBytes)->toBeGreaterThan(0)
                    ->and($result->writtenBytes)->toBeLessThanOrEqual(strlen($payload));
                $sent += $result->writtenBytes;
            }
            expect($blocked)->toBeTrue()->and($sent)->toBeGreaterThan(0);
            $drained = 0;
            for ($i = 0; $i < 2048 && $drained < $sent; $i++) {
                $bytes = fread($resources[1], 65_536);
                expect(is_string($bytes))->toBeTrue();
                if ($bytes === '') {
                    break;
                }
                expect($bytes)->toBe(str_repeat('x', strlen($bytes)));
                $drained += strlen($bytes);
            }
            expect($drained)->toBe($sent)
                ->and($socket->write('resume')->writtenBytes)->toBe(6)
                ->and(fread($resources[1], 6))->toBe('resume');
        } finally {
            $socket->close();
            fclose($resources[1]);
        }
    });


    it('returns null for an empty accept queue even with a warning-to-exception handler', function () {
        $resource = stream_socket_server('tcp://127.0.0.1:0');
        expect($resource)->not->toBeFalse();
        $listener = new Componenta\WebSocket\Socket\StreamSocketListener($resource);
        $handler = static function (int $level, string $message): never {
            throw new ErrorException($message, 0, $level);
        };

        set_error_handler($handler, E_WARNING | E_USER_WARNING);
        try {
            expect($listener->accept())->toBeNull();
            expect(fn () => trigger_error('caller warning handler', E_USER_WARNING))
                ->toThrow(ErrorException::class, 'caller warning handler');
        } finally {
            restore_error_handler();
            $listener->close();
        }
    });

});
