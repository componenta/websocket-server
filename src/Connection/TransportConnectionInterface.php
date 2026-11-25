<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Connection;

use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;
use Componenta\WebSocket\Socket\SocketInterface;

interface TransportConnectionInterface extends ConnectionInterface
{
    public SocketInterface $socket { get; }

    /** Stable restricted connection handle passed to application callbacks. */
    public ConnectionInterface $application { get; }

    /** Notify the runtime when an outgoing queue becomes nonempty or closing begins. */
    public function setWriteNotifier(?\Closure $notifier): void;

    public bool $needsWrite { get; }

    public int $queuedBytes { get; }

    public bool $closing { get; }

    public bool $readyToClose { get; }

    /** Fail the protocol and stop accepting further application messages. */
    public function failProtocol(int $code, string $reason): SendResult;

    public bool $closed { get; }

    public int $receivedPongs { get; }

    /**
     * @return list<Message>
     */
    public function appendBytes(string $bytes): array;

    public function ping(string $payload = ''): SendResult;

    public function flush(): bool;

    public function markClosed(?CloseInfo $close = null): void;
}
