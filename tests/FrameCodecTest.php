<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Exception\ProtocolException;
use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Handshake\HandshakeRequest;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Protocol\Opcode;
use Componenta\WebSocket\Socket\StreamSocket;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Transport\Server;

describe('WebSocket frame codec', function () {
    it('decodes masked client text frames', function () {
        $codec = new FrameCodec();
        $buffer = $codec->encode(new Frame(Opcode::TEXT, 'hello'), masked: true, maskKey: 'mask');

        $frame = $codec->decode($buffer);

        expect($frame)->toBeInstanceOf(Frame::class)
            ->and($frame->opcode)->toBe(Opcode::TEXT)
            ->and($frame->payload)->toBe('hello')
            ->and($buffer)->toBe('');
    });

    it('returns null for incomplete frames without consuming bytes', function () {
        $codec = new FrameCodec();
        $buffer = substr($codec->encode(new Frame(Opcode::TEXT, 'hello'), masked: true, maskKey: 'mask'), 0, -1);
        $original = $buffer;

        expect($codec->decode($buffer))->toBeNull()
            ->and($buffer)->toBe($original);
    });

    it('decodes unmasked server frames when expected', function () {
        $codec = new FrameCodec();
        $buffer = $codec->encode(new Frame(Opcode::BINARY, "\x01\x02"));

        $frame = $codec->decode($buffer, expectMasked: false);

        expect($frame)->toBeInstanceOf(Frame::class)
            ->and($frame->opcode)->toBe(Opcode::BINARY)
            ->and($frame->payload)->toBe("\x01\x02");
    });

    it('rejects unmasked client frames', function () {
        $codec = new FrameCodec();
        $buffer = $codec->encode(new Frame(Opcode::TEXT, 'hello'));

        expect(fn() => $codec->decode($buffer))->toThrow(ProtocolException::class);
    });

    it('rejects non-minimal extended payload lengths', function () {
        $codec = new FrameCodec();
        $minimal = $codec->encode(new Frame(Opcode::TEXT, 'hello'), masked: true, maskKey: 'mask');
        $buffer = chr(0x81) . chr(0x80 | 126) . pack('n', 5) . substr($minimal, 2);

        expect(fn() => $codec->decode($buffer))->toThrow(ProtocolException::class);
    });

    it('reassembles fragmented messages in connection order', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::TEXT, 'hel', final: false), masked: true, maskKey: 'mask')
            . $codec->encode(new Frame(Opcode::CONTINUATION, 'lo'), masked: true, maskKey: 'key!');

        $messages = $connection->appendBytes($bytes);

        expect($messages)->toHaveCount(1)
            ->and($messages[0]->opcode)->toBe(Opcode::TEXT)
            ->and($messages[0]->payload)->toBe('hello');

        $socket->close();
    });

    it('rejects fragmented messages over the aggregate message payload limit', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(5),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::TEXT, 'abc', final: false), masked: true, maskKey: 'mask')
            . $codec->encode(new Frame(Opcode::CONTINUATION, 'def'), masked: true, maskKey: 'key!');

        expect(fn() => $connection->appendBytes($bytes))->toThrow(ProtocolException::class);

        $socket->close();
    });

    it('rejects malformed close frame payloads', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::CLOSE, "\x03"), masked: true, maskKey: 'mask');

        expect(fn() => $connection->appendBytes($bytes))->toThrow(ProtocolException::class);

        $socket->close();
    });

    it('rejects invalid close frame codes', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::CLOSE, pack('n', 1006)), masked: true, maskKey: 'mask');

        expect(fn() => $connection->appendBytes($bytes))->toThrow(ProtocolException::class);

        $socket->close();
    });

    it('rejects non-UTF-8 close frame reasons', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::CLOSE, pack('n', 1000) . "\xC3\x28"), masked: true, maskKey: 'mask');

        expect(fn() => $connection->appendBytes($bytes))->toThrow(ProtocolException::class);

        $socket->close();
    });

    it('rejects non-UTF-8 text message payloads', function () {
        $codec = new FrameCodec();
        $socket = new StreamSocket(fopen('php://temp', 'r+'), '127.0.0.1:12345');
        $connection = new Connection(
            socket: $socket,
            codec: $codec,
            assembler: new MessageAssembler(1024),
            maxPayloadSize: 1024,
            id: 'test',
            request: new HandshakeRequest('GET', '/ws', '/ws', []),
        );
        $bytes = $codec->encode(new Frame(Opcode::TEXT, "\xC3\x28"), masked: true, maskKey: 'mask');

        $error = null;

        try {
            $connection->appendBytes($bytes);
        } catch (ProtocolException $e) {
            $error = $e;
        }

        expect($error)->toBeInstanceOf(ProtocolException::class)
            ->and($error?->closeCode)->toBe(CloseCode::INVALID_PAYLOAD_DATA);

        $socket->close();
    });
});
