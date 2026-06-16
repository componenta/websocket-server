# Componenta WebSocket Server

WebSocket-сервер, протокол, сокеты и примитивы соединений для Componenta. Пакет содержит низкоуровневую серверную среду выполнения и контракты приложения; `componenta/websocket-app` подключает его к процессу загрузки Componenta-приложения.

## Установка

```bash
composer require componenta/websocket-server
```

Пакет требует PHP `^8.4`, `componenta/config`, `componenta/http`, `psr/clock` и PSR-11.

## Контракт приложения

Реализуйте `WebSocketApplicationInterface`, чтобы обрабатывать события жизненного цикла:

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

Соединения раскрывают данные и контролируемые методы записи через `ConnectionInterface`:

| API | Значение |
|---|---|
| `$connection->id` | Внутренний id соединения. |
| `$connection->remoteAddress` | Адрес удалённого сокета. |
| `$connection->request` | Разобранный WebSocket-запрос рукопожатия. |
| `$connection->context` | Изменяемый объект контекста соединения. |
| `$connection->state` | Текущее состояние соединения. |
| `$connection->send(Message|string $message)` | Ставит в очередь существующий `Message` или текстовую строку. |
| `$connection->sendText(string $payload)` | Ставит в очередь текстовое сообщение. |
| `$connection->sendBinary(string $payload)` | Ставит в очередь бинарное сообщение. |
| `$connection->close(int $code, string $reason)` | Ставит в очередь кадр закрытия и переводит соединение к закрытию. |

Методы записи возвращают `SendResult`. Проверяйте `$result->accepted`, прежде чем считать сообщение поставленным в очередь; отказ содержит `$reason` и текущее значение `$queuedBytes`.

## Маршрутизация сообщений

Если приложению нужна только маршрутизация входящих сообщений, используйте `RoutedWebSocketApplication` с `MessageRouterInterface`:

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

`RoutedWebSocketApplication` добавляет соединение в реестр при подключении, передаёт входящие сообщения роутеру и удаляет соединение при отключении. Исключения приложения обрабатывает `SafeWebSocketApplicationInvoker`: он вызывает `WebSocketApplicationInterface::failed()` и не выпускает ошибки обработчика наружу в цикл сервера.

## Конфигурация

`WebSocketOptions::fromConfig()` читает ключи из `Componenta\WebSocket\Config\ConfigKey`:

| Ключ | Значение по умолчанию |
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

Дополнительные ключи управляют размером исходящего буфера, ожидающими рукопожатиями, временем простоя, временем мягкого завершения, тайм-аутом `select` и размером очереди `listen`.

## Что регистрирует пакет

`Componenta\WebSocket\ConfigProvider` регистрирует стандартные реализации для:

- `WebSocketServerInterface`
- `WebSocketOptionsInterface`
- `HandshakeInterface`
- `ConnectionFactoryInterface`
- `EventLoopInterface`
- `SocketListenerFactoryInterface`
- `SocketSelectorInterface`
- `WebSocketApplicationResolverInterface`
- `WebSocketApplicationInvokerInterface`

Стандартный сервер использует потоковые сокеты и цикл событий на основе `select`.

## Граница пакета

Пакет не предоставляет точки входа приложения, не загружает `config/websocket.php` и не регистрирует загрузчик Componenta. Для интеграции с Componenta-приложением используйте `componenta/websocket-app`.

Контракты слоя супервизора, например `WorkerInterface`, `WorkerRegistryInterface` и `WorkerMessageBusInterface`, объявлены здесь, потому что описывают среду выполнения сокет-сервера. Конкретный многопроцессный супервизор относится к приложению или слою развёртывания.
