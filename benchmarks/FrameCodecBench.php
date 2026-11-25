<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Opcode;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('setUp')]
final class FrameCodecBench
{
    private FrameCodec $codec;
    private Frame $small;
    private Frame $large;
    private string $smallWire;
    private string $largeWire;

    public function setUp(): void
    {
        $this->codec = new FrameCodec();
        $this->small = new Frame(Opcode::BINARY, str_repeat("\x01\xfe\x42\xab", 16));
        $this->large = new Frame(Opcode::BINARY, str_repeat("\x01\xfe\x42\xab", 262_144));
        $this->smallWire = $this->codec->encode($this->small, true, 'mask');
        $this->largeWire = $this->codec->encode($this->large, true, 'mask');
    }

    #[Bench\Revs(20_000)]
    public function benchEncodeSmallMasked(): void
    {
        $wire = $this->codec->encode($this->small, true, 'mask');
        if ($wire !== $this->smallWire) {
            throw new \RuntimeException('Incorrect encoded frame.');
        }
    }

    #[Bench\Revs(20_000)]
    public function benchDecodeSmallMasked(): void
    {
        $wire = $this->smallWire;
        $this->checkDecoded($this->codec->decode($wire), $this->small, $wire);
    }

    #[Bench\Revs(15)]
    public function benchRoundTripLargeMasked(): void
    {
        $wire = $this->codec->encode($this->large, true, 'mask');
        $this->checkDecoded($this->codec->decode($wire), $this->large, $wire);
    }

    #[Bench\Revs(100)]
    public function benchEncodeLargeUnmasked(): void
    {
        $wire = $this->codec->encode($this->large);
        if (strlen($wire) !== 1_048_586 || substr($wire, 0, 10) !== "\x82\x7f\x00\x00\x00\x00\x00\x10\x00\x00") {
            throw new \RuntimeException('Incorrect large frame.');
        }
    }

    #[Bench\Revs(15)]
    public function benchDecodeLargeMasked(): void
    {
        $wire = $this->largeWire;
        $this->checkDecoded($this->codec->decode($wire), $this->large, $wire);
    }

    private function checkDecoded(?Frame $decoded, Frame $expected, string $remaining): void
    {
        if ($decoded?->opcode !== $expected->opcode || $decoded->payload !== $expected->payload || $remaining !== '') {
            throw new \RuntimeException('Frame round trip failed.');
        }
    }
}
