# Componenta WebSocket Server

WebSocket server, protocol, socket, and connection primitives for Componenta. The package contains the low-level server runtime and application contracts; `componenta/websocket-app` connects it to the Componenta application boot process.

## Installation

```bash
composer require componenta/websocket-server
```

The package requires PHP `^8.4`, `componenta/arrayable`, `componenta/config`, `componenta/http`, and PSR-11.

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

Application callbacks receive a stable, restricted `ConnectionInterface` handle, not the transport connection or its socket. Connection data is read-only except for the replaceable context:

| API | Meaning |
|---|---|
| `$connection->id` | Framework connection id. |
| `$connection->remoteAddress` | Remote socket address. |
| `$connection->request` | Parsed WebSocket handshake request. |
| `$connection->context` | Replaceable context; `with()` and `without()` return new contexts, without cloning stored objects. |
| `$connection->state` | Current connection state. |
| `$connection->closeInfo` | Close metadata, or `null` before closing begins. |
| `$connection->send(Message|string $message)` | Queue an existing `Message` or a text string. |
| `$connection->sendText(string $payload)` | Queue a text message. |
| `$connection->sendBinary(string $payload)` | Queue a binary message. |
| `$connection->close(int $code, string $reason)` | Queue a close frame and move the connection toward closing. |

Write methods return `SendResult`. An accepted result means queued, not delivered to the peer. Rejected results include a `$reason` and the current `$queuedBytes` value. Both per-connection and shared outgoing limits apply; handle backpressure rather than assuming sends succeed. Text must be valid UTF-8.

`close()` starts the closing handshake; the connection stops accepting application messages and writes. The runtime waits for the peer's close frame and drains pending bytes within the configured deadline. `CloseInfo::clean` in the disconnect callback is true only for a completed, non-failed closing handshake. Abnormal transport termination is reported as such. Status 1010 is client-only: the server rejects a request to send it and acknowledges an incoming 1010 close without echoing that status. When stopping, the listening socket is closed before existing sessions are drained. Queues on other active connections are scheduled for writing even when those connections are idle.

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
| `MAX_OUTGOING_BUFFER_SIZE` | `4194304` |
| `MAX_TOTAL_OUTGOING_BUFFER_SIZE` | `67108864` |
| `MAX_TOTAL_INCOMING_BUFFER_SIZE` | `67108864` |
| `MAX_PENDING_HANDSHAKE_BYTES` | `8192` |
| `PENDING_HANDSHAKE_TIMEOUT_MS` | `10000` |
| `MAX_PENDING_CONNECTIONS` | `128` |
| `IDLE_TIMEOUT_MS` | `0` |
| `CLOSE_HANDSHAKE_TIMEOUT_MS` | `5000` |
| `SHUTDOWN_DRAIN_TIMEOUT_MS` | `1000` |
| `SELECT_TIMEOUT_USEC` | `200000` |
| `BACKLOG` | `128` |

The shared outgoing limit counts queued wire bytes for established connections within a single server run, not total process memory. The shared incoming limit counts undecoded bytes and retained fragmented payloads across established connections, including each newly received chunk before parsing. Exhaustion fails the affected connection with code 1009 when a close frame can be queued. It does not account for PHP object overhead, temporary copies or data retained by the application. Frame and message payload limits also apply. Pending handshakes have their own size, count and absolute time limits; receiving more bytes does not reset their deadline. Heartbeats use matching Ping/Pong payloads, and the Pong timeout starts when the Ping has been fully written. Time intervals use a monotonic clock. Heartbeat and idle checks are periodic; their observed timeout may be later than the configured interval because of the scan cadence, I/O scheduling or synchronous application callbacks. A zero heartbeat interval or idle timeout disables that feature; a zero shutdown drain timeout disables waiting.

The default `ALLOWED_ORIGINS` permits any origin for compatibility. For browser-facing deployments configure an explicit allowlist of trusted origins. An empty list rejects all supplied origins; an absent Origin is still permitted for non-browser clients. Invalid origin entries are not silently converted to a wildcard. Origin validation is not authentication. The validated origin list and parsed handshake header values are independent of direct PHP references passed by callers.

## Registered Services

`Componenta\WebSocket\ConfigProvider` registers defaults for:

- `WebSocketServerInterface`
- `WebSocketOptionsInterface`
- `HandshakeInterface`
- `ConnectionFactoryInterface`
- `EventLoopInterface`
- `MonotonicClockInterface`
- `SocketListenerFactoryInterface`
- `SocketSelectorInterface`
- `WebSocketApplicationResolverInterface`
- `WebSocketApplicationInvokerInterface`

The default server uses stream sockets and a select-based event loop. IPv6 bind addresses may be supplied with or without brackets. A stopped server does not stop unrelated loop work. Registered timers, watchers, their owning event loop, and live stream socket and listener wrappers cannot be cloned. Default stream wrappers reject resources that refuse nonblocking mode; on construction failure the supplied resource remains owned by its caller. Pending handshake read errors are reported without announcing an application connection. Native stream I/O failures return typed error results even with a caller diagnostic handler installed; that handler is restored after the operation. Shutdown I/O errors are reported separately from ordinary EOF. Error reporting is best effort: failure of the reporter does not stop other sessions or interrupt cleanup. `MAX_CONNECTIONS` is an application admission limit, not a guarantee that the operating system or `stream_select()` can support that many file descriptors. Unselectable sockets are rejected at admission. For larger deployments, provide a compatible selector or event loop.

## Migrating custom runtime implementations

Application callback signatures and `ConnectionInterface` are unchanged. Applications must not cast their connection handles to a concrete transport class or depend on socket operations.

Custom runtime adapters need to implement the following contracts:

| Contract | Migration |
|---|---|
| `MonotonicClockInterface` | Implement `nowMilliseconds(): float` as an elapsed-time source, not wall-clock time. The default uses `hrtime()`; a PSR-20 clock is no longer required. |
| `TransportConnectionInterface` | Provide the stable restricted `application` handle, `setWriteNotifier()`, `readyToClose`, `failProtocol()`, `awaitingPong` and `pingSent`. Replace `appendBytes()` with `receiveBytes(string $bytes, Closure $onMessage): void`; dispatch messages synchronously in wire order before processing later frames. Notify the runtime when the queue becomes nonempty or closing begins. |
| `PendingConnectionInterface` | Use `append(string $bytes)` and `expired(float $nowMs, int $timeoutMs)`; the former activity timestamps and redundant remote address are no longer exposed. |
| `ConnectionFactoryInterface` | Accept optional shared `BufferBudget` instances for outgoing and incoming bytes; enforce reservations until the bytes are sent, consumed or discarded. Message assemblers must be dedicated to a connection and report retained payload bytes via `bufferedBytes`. |
| `WebSocketApplicationInvokerInterface` | Return a boolean from `connected()` and `received()` to indicate callback completion; false indicates reported failure. |
| `SocketSelectorInterface` | Implement `assertSelectable()` to reject unusable targets before registering watchers. |
| `WebSocketOptionsInterface` | Provide `closeHandshakeTimeoutMs`, `maxTotalOutgoingBufferSize` and `maxTotalIncomingBufferSize`. |

## Boundary

This package does not provide application entry points, `config/websocket.php` loading, or Componenta bootloader registration. Use `componenta/websocket-app` for Componenta application integration.

Supervisor contracts such as `WorkerInterface`, `WorkerRegistryInterface`, and `WorkerMessageBusInterface` are declared here because they describe the socket server runtime. A concrete multi-process supervisor is an application/deployment concern.

## Development checks

Install development dependencies and run `vendor/bin/pest`. With Xdebug enabled, `XDEBUG_MODE=coverage vendor/bin/pest --coverage --coverage-clover=coverage.xml` measures in-process source line coverage. See [benchmarks](benchmarks/README.md), [targeted mutation checks](tests/Mutation/README.md) and [TCP stress acceptance](tests/Stress/README.md). Subprocess workloads are not included in the in-process coverage report.
