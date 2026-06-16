<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

interface FrameCodecInterface
{
    public function encodedSize(Frame $frame, bool $masked = false): int;

    public function encode(Frame $frame, bool $masked = false, ?string $maskKey = null): string;

    public function decode(
        string &$buffer,
        bool $expectMasked = true,
        int $maxPayloadSize = 1_048_576,
    ): ?Frame;
}
