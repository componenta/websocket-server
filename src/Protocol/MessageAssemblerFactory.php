<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

final readonly class MessageAssemblerFactory implements MessageAssemblerFactoryInterface
{
    public function create(int $maxMessagePayloadSize): MessageAssemblerInterface
    {
        return new MessageAssembler($maxMessagePayloadSize);
    }
}
