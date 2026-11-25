<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Loop\MonotonicClockInterface;
use Componenta\WebSocket\Protocol\CloseCode;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Socket\SocketReadStatus;

/** @internal Drains nonblocking close handshakes within a shared shutdown deadline. */
final readonly class ShutdownCoordinator
{
    public function __construct(private MonotonicClockInterface $clock) {}

    /**
     * @param \Closure(ActiveSession, CloseInfo): void $close
     * @param \Closure(ActiveSession, \Throwable): void $report
     */
    public function drain(ServerRunState $state, int $timeoutMs, \Closure $close, \Closure $report): void
    {
        $deadline = $this->clock->nowMilliseconds() + $timeoutMs;

        foreach ($state->active() as $session) {
            $connection = $session->connection;
            try {
                $connection->setWriteNotifier(null);
                if (!$connection->closing && !$connection->closed
                    && !$connection->close(CloseCode::GOING_AWAY, 'Server shutdown')->accepted
                ) {
                    $close($session, CloseInfo::transport('Unable to queue shutdown close frame.'));
                }
            } catch (\Throwable $error) {
                $report($session, $error);
                $close($session, CloseInfo::transport('Unable to begin shutdown close.'));
            }
        }

        while ($state->activeCount() > 0 && $this->clock->nowMilliseconds() < $deadline) {
            $progress = false;
            foreach ($state->active() as $session) {
                if ($this->clock->nowMilliseconds() >= $deadline) {
                    break;
                }

                $connection = $session->connection;
                try {
                    $queuedBefore = $connection->queuedBytes;
                    if ($connection->needsWrite && !$connection->flush()) {
                        throw new \RuntimeException('Unable to write shutdown close frame.');
                    }
                    $progress = $progress || $connection->queuedBytes < $queuedBefore;

                    if (!$connection->readyToClose) {
                        $read = $connection->socket->read(8192);
                        if ($read->status === SocketReadStatus::DATA) {
                            $connection->appendBytes($read->bytes);
                            $progress = true;
                        } elseif ($read->status === SocketReadStatus::ERROR || $read->status === SocketReadStatus::CLOSED) {
                            $close($session, CloseInfo::transport($read->reason ?? 'Socket closed during shutdown.'));
                            continue;
                        }
                    }

                    if ($connection->readyToClose) {
                        $close($session, $connection->closeInfo ?? CloseInfo::serverShutdown());
                    }
                } catch (\Throwable $error) {
                    $report($session, $error);
                    $close($session, CloseInfo::transport('WebSocket shutdown I/O failed.'));
                }
            }

            if (!$progress && $state->activeCount() > 0) {
                $remainingUsec = (int) (($deadline - $this->clock->nowMilliseconds()) * 1000);
                if ($remainingUsec > 0) {
                    usleep(min(1000, $remainingUsec));
                }
            }
        }

        foreach ($state->active() as $session) {
            $close($session, $session->connection->closeInfo ?? CloseInfo::serverShutdown());
        }
    }
}
