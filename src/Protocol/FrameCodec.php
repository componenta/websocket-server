<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

use Componenta\WebSocket\Exception\ProtocolException;

final class FrameCodec implements FrameCodecInterface
{
    public function encodedSize(Frame $frame, bool $masked = false): int
    {
        $length = strlen($frame->payload);
        $headerLength = match (true) {
            $length <= 125 => 2,
            $length <= 65_535 => 4,
            default => 10,
        };

        return $headerLength + ($masked ? 4 : 0) + $length;
    }

    public function encode(Frame $frame, bool $masked = false, ?string $maskKey = null): string
    {
        $payload = $frame->payload;
        $length = strlen($payload);
        $first = ($frame->final ? 0x80 : 0x00) | $frame->opcode->value;
        $maskBit = $masked ? 0x80 : 0x00;

        if ($length <= 125) {
            $header = pack('CC', $first, $maskBit | $length);
        } elseif ($length <= 65_535) {
            $header = pack('CCn', $first, $maskBit | 126, $length);
        } else {
            $header = pack('CCNN', $first, $maskBit | 127, 0, $length);
        }

        if (!$masked) {
            return $header . $payload;
        }

        $maskKey ??= random_bytes(4);

        if (strlen($maskKey) !== 4) {
            throw new \InvalidArgumentException('WebSocket mask key must be exactly 4 bytes.');
        }

        return $header . $maskKey . $this->mask($payload, $maskKey);
    }

    public function decode(
        string &$buffer,
        bool $expectMasked = true,
        int $maxPayloadSize = 1_048_576,
    ): ?Frame {
        $bufferLength = strlen($buffer);

        if ($bufferLength < 2) {
            return null;
        }

        $first = ord($buffer[0]);
        $second = ord($buffer[1]);
        $final = ($first & 0x80) !== 0;
        $rsv = $first & 0x70;
        $opcode = Opcode::tryFrom($first & 0x0F);
        $masked = ($second & 0x80) !== 0;
        $payloadLength = $second & 0x7F;
        $offset = 2;

        if ($rsv !== 0) {
            throw new ProtocolException('Reserved WebSocket frame bits are not supported.');
        }

        if ($opcode === null) {
            throw new ProtocolException('Unknown WebSocket opcode.');
        }

        if ($expectMasked && !$masked) {
            throw new ProtocolException('Client WebSocket frames must be masked.');
        }

        if (!$expectMasked && $masked) {
            throw new ProtocolException('Server WebSocket frames must not be masked.');
        }

        if ($payloadLength === 126) {
            if ($bufferLength < $offset + 2) {
                return null;
            }

            $payloadLength = unpack('n', substr($buffer, $offset, 2))[1];

            if ($payloadLength < 126) {
                throw new ProtocolException('WebSocket payload uses non-minimal length encoding.');
            }

            $offset += 2;
        } elseif ($payloadLength === 127) {
            if ($bufferLength < $offset + 8) {
                return null;
            }

            $parts = unpack('Nhigh/Nlow', substr($buffer, $offset, 8));

            if ($parts['high'] !== 0) {
                throw new ProtocolException(
                    'WebSocket payload is too large.',
                    CloseCode::PAYLOAD_TOO_LARGE,
                );
            }

            $payloadLength = $parts['low'];

            if ($payloadLength < 65_536) {
                throw new ProtocolException('WebSocket payload uses non-minimal length encoding.');
            }

            $offset += 8;
        }

        if ($payloadLength > $maxPayloadSize) {
            throw new ProtocolException(
                'WebSocket payload exceeds configured maximum size.',
                CloseCode::PAYLOAD_TOO_LARGE,
            );
        }

        if ($opcode->isControl()) {
            if (!$final) {
                throw new ProtocolException('WebSocket control frames must not be fragmented.');
            }

            if ($payloadLength > 125) {
                throw new ProtocolException('WebSocket control frame payload exceeds 125 bytes.');
            }
        }

        $maskLength = $masked ? 4 : 0;

        if ($bufferLength < $offset + $maskLength + $payloadLength) {
            return null;
        }

        $maskKey = $masked ? substr($buffer, $offset, 4) : null;
        $offset += $maskLength;

        $payload = substr($buffer, $offset, $payloadLength);
        $offset += $payloadLength;

        $buffer = substr($buffer, $offset);

        if ($maskKey !== null) {
            $payload = $this->mask($payload, $maskKey);
        }

        return new Frame($opcode, $payload, $final);
    }

    private function mask(string $payload, string $maskKey): string
    {
        $length = strlen($payload);

        for ($i = 0; $i < $length; $i++) {
            $payload[$i] = $payload[$i] ^ $maskKey[$i % 4];
        }

        return $payload;
    }
}
