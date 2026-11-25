<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Benchmarks;

use Componenta\WebSocket\Protocol\Frame;
use Componenta\WebSocket\Protocol\MessageAssembler;
use Componenta\WebSocket\Protocol\Opcode;
use PhpBench\Attributes as Bench;

#[Bench\Groups(['comparative'])]
#[Bench\BeforeMethods('setUp')]
final class MessageAssemblerBench
{
    private MessageAssembler $assembler;
    private Frame $text;
    /** @var list<Frame> */
    private array $fragments;
    private string $expected;

    public function setUp(): void
    {
        $this->assembler = new MessageAssembler(1_048_576);
        $this->text = new Frame(Opcode::TEXT, str_repeat('a', 64));
        $chunk = str_repeat('x', 8_192);
        $this->expected = str_repeat($chunk, 8);
        $this->fragments = [new Frame(Opcode::BINARY, $chunk, false)];
        for ($i = 1; $i < 8; $i++) {
            $this->fragments[] = new Frame(Opcode::CONTINUATION, $chunk, $i === 7);
        }
    }

    #[Bench\Revs(20_000)]
    public function benchCompleteText(): void
    {
        $message = $this->assembler->push($this->text);
        if ($message?->payload !== $this->text->payload || $message->opcode !== Opcode::TEXT) {
            throw new \RuntimeException('Incorrect text assembly.');
        }
    }

    #[Bench\Revs(2_000)]
    public function benchFragmentedBinary64KiB(): void
    {
        foreach ($this->fragments as $index => $frame) {
            $message = $this->assembler->push($frame);
            if ($index !== 7 && $message !== null) {
                throw new \RuntimeException('Premature message delivery.');
            }
        }

        if ($message?->payload !== $this->expected || $message->opcode !== Opcode::BINARY) {
            throw new \RuntimeException('Incorrect fragmented assembly.');
        }
    }
}
