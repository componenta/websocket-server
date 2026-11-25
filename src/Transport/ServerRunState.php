<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Transport;

use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Connection\BufferBudget;

/** @internal The ownership boundary for one invocation of Server::run(). */
final class ServerRunState
{
    /** @var array<int|string, PendingSession> */
    private array $pending = [];
    /** @var array<int|string, ActiveSession> */
    private array $active = [];

    public readonly SessionWatchers $watchers;
    public readonly BufferBudget $outgoingBudget;
    public readonly BufferBudget $incomingBudget;

    public function __construct(
        public readonly WebSocketApplicationInterface $application,
        int $maxOutgoingBytes,
        int $maxIncomingBytes,
    ) {
        $this->watchers = new SessionWatchers();
        $this->outgoingBudget = new BufferBudget($maxOutgoingBytes);
        $this->incomingBudget = new BufferBudget($maxIncomingBytes);
    }

    public function pendingCount(): int
    {
        return count($this->pending);
    }

    public function activeCount(): int
    {
        return count($this->active);
    }

    public function addPending(PendingSession $session): void
    {
        $id = $session->connection->socket->id;
        if (isset($this->pending[$id]) || isset($this->active[$id])) {
            throw new \LogicException('Socket id is already registered.');
        }
        $this->pending[$id] = $session;
    }

    public function hasPending(PendingSession $session): bool
    {
        return ($this->pending[$session->connection->socket->id] ?? null) === $session;
    }

    public function hasActive(ActiveSession $session): bool
    {
        return ($this->active[$session->connection->socket->id] ?? null) === $session;
    }

    public function activate(PendingSession $pending, ActiveSession $active): void
    {
        $id = $pending->connection->socket->id;
        if (!$this->hasPending($pending) || $active->connection->socket !== $pending->connection->socket) {
            throw new \LogicException('Active session must own the pending socket.');
        }

        unset($this->pending[$id]);
        $this->active[$id] = $active;
    }

    public function removePending(PendingSession $session): bool
    {
        if (!$this->hasPending($session)) {
            return false;
        }

        unset($this->pending[$session->connection->socket->id]);
        return true;
    }

    public function removeActive(ActiveSession $session): bool
    {
        if (!$this->hasActive($session)) {
            return false;
        }

        unset($this->active[$session->connection->socket->id]);
        return true;
    }

    /** @return iterable<PendingSession> */
    public function pending(): iterable
    {
        yield from $this->pending;
    }

    /** @return iterable<ActiveSession> */
    public function active(): iterable
    {
        yield from $this->active;
    }
}
