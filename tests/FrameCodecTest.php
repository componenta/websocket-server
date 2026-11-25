<?php

declare(strict_types=1);

use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Connection\Connection;
use Componenta\WebSocket\Tests\Support\ReceivedMessages;
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

        $messages = ReceivedMessages::collect($connection, $bytes);

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

        expect(fn() => ReceivedMessages::collect($connection, $bytes))->toThrow(ProtocolException::class);

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

        expect(fn() => ReceivedMessages::collect($connection, $bytes))->toThrow(ProtocolException::class);

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

        expect(fn() => ReceivedMessages::collect($connection, $bytes))->toThrow(ProtocolException::class);

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

        expect(fn() => ReceivedMessages::collect($connection, $bytes))->toThrow(ProtocolException::class);

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
            ReceivedMessages::collect($connection, $bytes);
        } catch (ProtocolException $e) {
            $error = $e;
        }

        expect($error)->toBeInstanceOf(ProtocolException::class)
            ->and($error?->closeCode)->toBe(CloseCode::INVALID_PAYLOAD_DATA);

        $socket->close();
    });

    it('matches the RFC 6455 masked Hello frame byte for byte', function () {
        $codec = new FrameCodec();

        $encoded = $codec->encode(new Frame(Opcode::TEXT, 'Hello'), true, hex2bin('37fa213d'));

        expect(bin2hex($encoded))->toBe('818537fa213d7f9f4d5158');
    });

    it('preserves payload bytes for all mask and length boundaries', function (int $length) {
        $codec = new FrameCodec();
        $payload = substr(str_repeat("\x00\xff\x42\xab\x17", intdiv($length + 4, 5)), 0, $length);
        $key = "\xff\x00\x7f\x1a";
        $expectedMasked = '';
        for ($i = 0; $i < $length; $i++) {
            $expectedMasked .= $payload[$i] ^ $key[$i % 4];
        }
        $buffer = $codec->encode(new Frame(Opcode::BINARY, $payload), true, $key);

        expect(substr($buffer, -$length ?: strlen($buffer)))->toBe($expectedMasked)
            ->and($codec->decode($buffer)?->payload)->toBe($payload)
            ->and($buffer)->toBe('');
    })->with([0, 1, 3, 4, 5, 125, 126, 65535, 65536, 65537, 131075, 1048575, 1048576]);

    it('rejects a frame with the reserved most significant payload length bit set', function () {
        $codec = new FrameCodec();
        $buffer = "\x82\xff\x80\x00\x00\x00\x00\x00\x00\x00";
        $code = null;

        try {
            $codec->decode($buffer);
        } catch (ProtocolException $e) {
            $code = $e->closeCode;
        }

        expect($code)->toBe(CloseCode::PROTOCOL_ERROR);
    });

    it('rejects extended payload lengths over the configured maximum before reading a body', function () {
        $codec = new FrameCodec();
        $buffer = "\x82\xff\x00\x00\x00\x01\x00\x00\x00\x00";
        $original = $buffer;
        $error = null;

        try {
            $codec->decode($buffer, maxPayloadSize: 1_048_576);
        } catch (ProtocolException $exception) {
            $error = $exception;
        }

        expect($error?->closeCode)->toBe(CloseCode::PAYLOAD_TOO_LARGE)
            ->and($buffer)->toBe($original);
    });

    it('waits for a permitted extended payload without overflowing buffer length arithmetic', function () {
        $codec = new FrameCodec();
        $length = PHP_INT_SIZE >= 8
            ? "\x7f\xff\xff\xff\xff\xff\xff\xff"
            : "\x00\x00\x00\x00\x7f\xff\xff\xff";
        $buffer = "\x82\xff" . $length;
        $original = $buffer;

        expect($codec->decode($buffer, maxPayloadSize: PHP_INT_MAX))->toBeNull()
            ->and($buffer)->toBe($original);
    });

    it('unmasks a large frame without consuming a following partial frame', function (int $length) {
        $codec = new FrameCodec();
        $payload = substr(str_repeat("\x00\xfe\x11\xab\x17", intdiv($length + 4, 5)), 0, $length);
        $key = "\x12\x34\x56\x78";
        $masked = $payload ^ str_repeat($key, intdiv($length + 3, 4));
        $following = "\x81\x85mask";
        $buffer = "\x82\xff" . pack('NN', 0, $length) . $key . $masked . $following;

        expect($codec->decode($buffer)?->payload)->toBe($payload)
            ->and($buffer)->toBe($following)
            ->and($codec->decode($buffer))->toBeNull()
            ->and($buffer)->toBe($following);
    })->with([65_537, 131_075, 1_048_575]);

    it('resets a zero-byte fragmented message even when no payload bytes were retained', function () {
        $assembler = new MessageAssembler(1024);
        expect($assembler->push(new Frame(Opcode::TEXT, '', false)))->toBeNull()
            ->and($assembler->bufferedBytes)->toBe(0);
        expect(fn () => $assembler->push(new Frame(Opcode::TEXT, 'new')))
            ->toThrow(ProtocolException::class);
        $assembler->reset();
        expect($assembler->push(new Frame(Opcode::TEXT, 'new'))?->payload)->toBe('new');
    });

});
