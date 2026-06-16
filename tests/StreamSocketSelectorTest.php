<?php

declare(strict_types=1);

use Componenta\WebSocket\Socket\SocketInterface;
use Componenta\WebSocket\Socket\SocketReadResult;
use Componenta\WebSocket\Socket\SocketReadStatus;
use Componenta\WebSocket\Socket\SocketSelectionInterface;
use Componenta\WebSocket\Socket\SocketWriteResult;
use Componenta\WebSocket\Socket\SocketWriteStatus;
use Componenta\WebSocket\Socket\StreamSocketInterface;
use Componenta\WebSocket\Socket\StreamSocketListenerInterface;
use Componenta\WebSocket\Socket\StreamSocketSelector;
use Componenta\WebSocket\Transport\Server;

final class ContractBackedStreamSocket implements StreamSocketInterface
{
    public int|string $id {
        get => (int) $this->resource;
    }

    public bool $eof {
        get => feof($this->resource);
    }

    public function __construct(
        public private(set) readonly mixed $resource,
        public private(set) readonly string $remoteAddress,
    ) {
        stream_set_blocking($this->resource, false);
    }

    public function read(int $maxBytes): SocketReadResult
    {
        $bytes = @fread($this->resource, $maxBytes);

        if ($bytes === false) {
            return new SocketReadResult(SocketReadStatus::ERROR, reason: 'Unable to read from socket.');
        }

        if ($bytes === '') {
            return new SocketReadResult($this->eof ? SocketReadStatus::CLOSED : SocketReadStatus::EMPTY);
        }

        return new SocketReadResult(SocketReadStatus::DATA, $bytes);
    }

    public function write(string $bytes): SocketWriteResult
    {
        $written = @fwrite($this->resource, $bytes);

        if ($written === false) {
            return new SocketWriteResult(SocketWriteStatus::ERROR, reason: 'Unable to write to socket.');
        }

        if ($written === 0) {
            return new SocketWriteResult(SocketWriteStatus::BLOCKED);
        }

        return new SocketWriteResult(SocketWriteStatus::WRITTEN, $written);
    }

    public function close(): void
    {
        if (is_resource($this->resource)) {
            @fclose($this->resource);
        }
    }
}

final class ContractBackedStreamListener implements StreamSocketListenerInterface
{
    public int|string $id {
        get => (int) $this->resource;
    }

    public function __construct(
        public private(set) readonly mixed $resource,
    ) {
        stream_set_blocking($this->resource, false);
    }

    public function accept(): ?SocketInterface
    {
        $client = @stream_socket_accept($this->resource, 0, $remoteAddress);

        return $client === false
            ? null
            : new ContractBackedStreamSocket($client, $remoteAddress ?: '');
    }

    public function close(): void
    {
        if (is_resource($this->resource)) {
            @fclose($this->resource);
        }
    }
}

describe('WebSocket stream socket selector', function () {
    it('selects sockets through stream resource contracts instead of concrete implementations', function () {
        $listenerResource = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        expect($listenerResource)->not->toBeFalse("Unable to create listener: {$errorMessage} ({$errorCode}).");
        $address = stream_socket_get_name($listenerResource, false);
        expect($address)->not->toBeFalse();

        $clientResource = @stream_socket_client("tcp://{$address}", $errorCode, $errorMessage, 1);
        expect($clientResource)->not->toBeFalse("Unable to create client: {$errorMessage} ({$errorCode}).");
        $serverResource = @stream_socket_accept($listenerResource, 1, $remoteAddress);
        expect($serverResource)->not->toBeFalse();

        $listener = new ContractBackedStreamListener($listenerResource);
        $client = new ContractBackedStreamSocket($clientResource, (string) $address);
        $server = new ContractBackedStreamSocket($serverResource, $remoteAddress ?: '');

        try {
            expect($client->write('x')->status)->toBe(SocketWriteStatus::WRITTEN);

            $selection = (new StreamSocketSelector())->select([$listener], [$server], [$client], 100_000);

            expect($selection)->toBeInstanceOf(SocketSelectionInterface::class)
                ->and($selection->canRead($server))->toBeTrue()
                ->and($selection->canWrite($client))->toBeTrue();
        } finally {
            $server->close();
            $client->close();
            $listener->close();
        }
    });
});
