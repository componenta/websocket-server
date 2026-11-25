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

    /**
     * Notify the runtime when an outgoing queue becomes nonempty or closing begins.
     * The runtime may close this connection synchronously when registration fails.
     */
    public function setWriteNotifier(?\Closure $notifier): void;

    public bool $needsWrite { get; }

    public int $queuedBytes { get; }

    public bool $closing { get; }

    public bool $readyToClose { get; }

    /** Fail the protocol and stop accepting further application messages. */
    public function failProtocol(int $code, string $reason): SendResult;

    public bool $closed { get; }

    public int $receivedPongs { get; }

    /** A ping is queued or sent and has not received a matching pong. */
    public bool $awaitingPong { get; }

    /** The outstanding ping has been completely written to the socket. */
    public bool $pingSent { get; }

    /**
     * Process bytes in wire order and deliver complete messages synchronously.
     * The callback is not retained; recursive receives are not supported.
     * Further data messages are not delivered once closing begins.
     *
     * @param \Closure(Message): void $onMessage
     */
    public function receiveBytes(string $bytes, \Closure $onMessage): void;

    public function ping(string $payload = ''): SendResult;

    public function flush(): bool;

    public function markClosed(?CloseInfo $close = null): void;
}
