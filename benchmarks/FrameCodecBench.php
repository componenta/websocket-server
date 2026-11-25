<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\FrameCodec;
use Componenta\WebSocket\Protocol\Opcode;
use PhpBench\Attributes as Bench;

#[Bench\Groups(['comparative'])]
final class FrameCodecBench
{
    private FrameCodec $codec;
    private Frame $small;
    private Frame $large;
    private string $smallWire;
    private string $largeWire;

    public function setUpSmall(): void
    {
        $this->codec = new FrameCodec();
        $this->small = new Frame(Opcode::BINARY, str_repeat("\x01\xfe\x42\xab", 16));
        // Independent expected wire data; the decoder is not fed its own encoder output.
        $this->smallWire = "\x82\xc0mask" . str_repeat("\x6c\x9f\x31\xc0", 16);
    }

    public function setUpLarge(): void
    {
        $this->codec = new FrameCodec();
        $this->large = new Frame(Opcode::BINARY, str_repeat("\x01\xfe\x42\xab", 262_144));
        $this->largeWire = "\x82\xff\x00\x00\x00\x00\x00\x10\x00\x00mask"
            . str_repeat("\x6c\x9f\x31\xc0", 262_144);
    }

    #[Bench\BeforeMethods('setUpSmall')]
    #[Bench\Revs(20_000)]
    public function benchEncodeSmallMasked(): void
    {
        $wire = $this->codec->encode($this->small, true, 'mask');
        if ($wire !== $this->smallWire) {
            throw new \RuntimeException('Incorrect encoded frame.');
        }
    }

    #[Bench\BeforeMethods('setUpSmall')]
    #[Bench\Revs(20_000)]
    public function benchDecodeSmallMasked(): void
    {
        $wire = $this->smallWire;
        $this->checkDecoded($this->codec->decode($wire), $this->small, $wire);
    }

    #[Bench\BeforeMethods('setUpLarge')]
    #[Bench\Revs(15)]
    public function benchEncodeLargeMasked(): void
    {
        if ($this->codec->encode($this->large, true, 'mask') !== $this->largeWire) {
            throw new \RuntimeException('Incorrect encoded large masked frame.');
        }
    }

    #[Bench\BeforeMethods('setUpLarge')]
    #[Bench\Revs(15)]
    public function benchRoundTripLargeMasked(): void
    {
        $wire = $this->codec->encode($this->large, true, 'mask');
        $this->checkDecoded($this->codec->decode($wire), $this->large, $wire);
    }

    #[Bench\BeforeMethods('setUpLarge')]
    #[Bench\Revs(100)]
    public function benchEncodeLargeUnmasked(): void
    {
        $wire = $this->codec->encode($this->large);
        if (strlen($wire) !== 1_048_586 || substr($wire, 0, 10) !== "\x82\x7f\x00\x00\x00\x00\x00\x10\x00\x00"
            || substr_compare($wire, $this->large->payload, 10) !== 0
        ) {
            throw new \RuntimeException('Incorrect large frame.');
        }
    }

    #[Bench\BeforeMethods('setUpLarge')]
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
