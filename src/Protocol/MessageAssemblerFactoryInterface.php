<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

interface MessageAssemblerFactoryInterface
{
    public function create(int $maxMessagePayloadSize): MessageAssemblerInterface;
}
