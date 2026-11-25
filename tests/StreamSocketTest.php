<?php

declare(strict_types=1);

use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocket;

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

});
