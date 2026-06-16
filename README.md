# Componenta WebSocket Server

WebSocket server, protocol, socket, and connection primitives for Componenta. The package contains the low-level server runtime and application contracts; `componenta/websocket-app` connects it to the Componenta application boot process.

## Installation

```bash
composer require componenta/websocket-server
```

The package requires PHP `^8.4`, `componenta/config`, `componenta/http`, `psr/clock`, and PSR-11.

## Application Contract

Implement `WebSocketApplicationInterface` to handle lifecycle events:

```php
use Componenta\WebSocket\Application\WebSocketApplicationInterface;
use Componenta\WebSocket\Application\Error\WebSocketErrorContextInterface;
use Componenta\WebSocket\Connection\ConnectionInterface;
use Componenta\WebSocket\Protocol\CloseInfo;
use Componenta\WebSocket\Protocol\Message;

final class EchoApplication implements WebSocketApplicationInterface
{
    public function connected(ConnectionInterface $connection): void {}

    public function received(ConnectionInterface $connection, Message $message): void
    {
        $connection->send($message);
    }

    public function disconnected(ConnectionInterface $connection, CloseInfo $close): void {}

    public function failed(WebSocketErrorContextInterface $context): void {}
}
```

Connections expose immutable connection data and controlled write methods through `ConnectionInterface`:

| API | Meaning |
|---|---|
| `$connection->id` | Framework connection id. |
| `$connection->remoteAddress` | Remote socket address. |
| `$connection->request` | Parsed WebSocket handshake request. |
| `$connection->context` | Mutable connection context object. |
| `$connection->state` | Current connection state. |
| `$connection->send(Message|string $message)` | Queue an existing `Message` or a text string. |
| `$connection->sendText(string $payload)` | Queue a text message. |
| `$connection->sendBinary(string $payload)` | Queue a binary message. |
| `$connection->close(int $code, string $reason)` | Queue a close frame and move the connection toward closing. |

Write methods return `SendResult`. Check `$result->accepted` before assuming the payload was queued; rejected results include a `$reason` and the current `$queuedBytes` value.

## Message Routing

For applications that only need message dispatch, use `RoutedWebSocketApplication` with a `MessageRouterInterface`:

```php
use Componenta\WebSocket\Application\CallableMessageRouter;
use Componenta\WebSocket\Application\InMemoryConnectionRegistry;
use Componenta\WebSocket\Application\RoutedWebSocketApplication;

$app = new RoutedWebSocketApplication(
    new CallableMessageRouter(static function ($connection, $message): void {
        $connection->send($message);
    }),
    new InMemoryConnectionRegistry(),
);
```

`RoutedWebSocketApplication` adds connections to the registry on connect, delegates incoming messages to the router, and removes connections on disconnect. Application exceptions are reported through `SafeWebSocketApplicationInvoker`, which calls `WebSocketApplicationInterface::failed()` instead of letting handler failures escape the server loop.

## Configuration

`WebSocketOptions::fromConfig()` reads keys from `Componenta\WebSocket\Config\ConfigKey`:

| Key | Default |
|---|---|
| `HOST` | `127.0.0.1` |
| `PORT` | `8080` |
| `PATH` | `/ws` |
| `ALLOWED_ORIGINS` | `['*']` |
| `MAX_FRAME_PAYLOAD_SIZE` | `1048576` |
| `MAX_MESSAGE_PAYLOAD_SIZE` | `1048576` |
| `HEARTBEAT_INTERVAL_MS` | `30000` |
| `PONG_TIMEOUT_MS` | `10000` |
| `MAX_CONNECTIONS` | `1024` |

Additional keys control outgoing buffer size, pending handshakes, idle timeout, shutdown drain timeout, select timeout, and listen backlog.

## Registered Services

`Componenta\WebSocket\ConfigProvider` registers defaults for:

- `WebSocketServerInterface`
- `WebSocketOptionsInterface`
- `HandshakeInterface`
- `ConnectionFactoryInterface`
- `EventLoopInterface`
- `SocketListenerFactoryInterface`
- `SocketSelectorInterface`
- `WebSocketApplicationResolverInterface`
- `WebSocketApplicationInvokerInterface`

The default server uses stream sockets and a select-based event loop.

## Boundary

This package does not provide application entry points, `config/websocket.php` loading, or Componenta bootloader registration. Use `componenta/websocket-app` for Componenta application integration.

Supervisor contracts such as `WorkerInterface`, `WorkerRegistryInterface`, and `WorkerMessageBusInterface` are declared here because they describe the socket server runtime. A concrete multi-process supervisor is an application/deployment concern.
