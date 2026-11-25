# Componenta WebSocket Server

WebSocket-сервер, протокол, сокеты и примитивы соединений для Componenta. Пакет содержит низкоуровневую серверную среду выполнения и контракты приложения; `componenta/websocket-app` подключает его к процессу загрузки Componenta-приложения.

## Установка

```bash
composer require componenta/websocket-server
```

Пакет требует PHP `^8.4`, `componenta/arrayable`, `componenta/config`, `componenta/http` и PSR-11.

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

Обработчики приложения получают стабильный ограниченный объект `ConnectionInterface`, а не транспортное соединение или его сокет. Данные соединения доступны только для чтения, за исключением заменяемого контекста:

| API | Значение |
|---|---|
| `$connection->id` | Внутренний id соединения. |
| `$connection->remoteAddress` | Адрес удалённого сокета. |
| `$connection->request` | Разобранный WebSocket-запрос рукопожатия. |
| `$connection->context` | Заменяемый контекст: `with()` и `without()` возвращают новые контексты, не клонируя содержащиеся в них объекты. |
| `$connection->state` | Текущее состояние соединения. |
| `$connection->closeInfo` | Сведения о закрытии или `null` до начала закрытия. |
| `$connection->send(Message|string $message)` | Ставит в очередь существующий `Message` или текстовую строку. |
| `$connection->sendText(string $payload)` | Ставит в очередь текстовое сообщение. |
| `$connection->sendBinary(string $payload)` | Ставит в очередь бинарное сообщение. |
| `$connection->close(int $code, string $reason)` | Ставит в очередь кадр закрытия и переводит соединение к закрытию. |

Методы записи возвращают `SendResult`. Принятие означает постановку в очередь, а не доставку получателю. Отказ содержит `$reason` и текущее значение `$queuedBytes`. Действуют индивидуальный и общий лимиты исходящих очередей; приложение должно обрабатывать отказы при переполнении. Текст должен быть корректным UTF-8.

`close()` запускает рукопожатие закрытия; соединение перестаёт принимать прикладные сообщения и операции записи. Среда выполнения ожидает кадр закрытия другой стороны и отправляет оставшиеся данные в пределах заданного срока. В обработчике отключения `CloseInfo::clean` равен `true` только при завершённом рукопожатии без ошибок. Аварийное завершение транспорта обозначается соответствующим образом. Код 1010 предназначен для клиента: сервер отклоняет попытку отправить его и подтверждает входящее закрытие с этим кодом без его повторения. При остановке слушающий сокет закрывается до завершения активных соединений. Запись в очереди других активных соединений планируется даже при отсутствии входящих событий на этих соединениях.

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

Общий лимит исходящих очередей учитывает байты данных в очередях установленных соединений одного запуска сервера, а не всё потребление памяти процесса. Общий входящий лимит учитывает неразобранные байты и накопленные фрагменты сообщений установленных соединений, включая очередную порцию данных до разбора. При исчерпании закрывается соответствующее соединение с кодом 1009, если кадр закрытия удаётся поставить в очередь. Накладные расходы PHP, временные копии и данные, удерживаемые приложением, в лимит не входят. Также действуют индивидуальные лимиты кадров и сообщений. Для ожидающих рукопожатий действуют отдельные ограничения размера, количества и абсолютного срока; новые байты не продлевают этот срок. При проверке доступности сопоставляются данные Ping и Pong, а срок ожидания Pong отсчитывается с момента полной отправки Ping. Интервалы измеряются монотонными часами. Проверка доступности и простоя выполняется периодически; фактическое срабатывание может произойти позже заданного срока из-за периодичности обхода, планирования ввода-вывода и синхронных обработчиков приложения. Нулевые интервалы проверки доступности и простоя отключают соответствующие функции; нулевой срок завершения отключает ожидание при остановке.

Для совместимости значение `ALLOWED_ORIGINS` по умолчанию разрешает любой источник. При работе с браузерными клиентами задавайте явный список доверенных источников. Пустой список отклоняет все запросы с указанным Origin; отсутствие Origin допускается для небраузерных клиентов. Некорректные элементы списка не заменяются разрешением всех источников. Проверка Origin не заменяет аутентификацию. Проверенный список источников и значения заголовков рукопожатия не сохраняют прямые PHP-ссылки на переданные вызывающим кодом переменные.

## Что регистрирует пакет

`Componenta\WebSocket\ConfigProvider` регистрирует стандартные реализации для:

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

Стандартный сервер использует потоковые сокеты и цикл событий на основе `select`. IPv6-адрес для прослушивания можно указать с квадратными скобками или без них. Остановка неработающего сервера не останавливает другие задачи цикла событий. Зарегистрированные таймеры, наблюдатели, владеющий ими цикл событий и объекты потоковых сокетов и слушателей нельзя клонировать. Стандартные реализации отклоняют ресурсы, для которых не удаётся установить неблокирующий режим; при ошибке конструктора ресурс остаётся во владении вызывающего кода. Ошибки чтения при рукопожатии передаются обработчику ошибок без объявления прикладного соединения. Ошибки потокового ввода-вывода возвращаются в виде типизированных результатов даже при установленном пользовательском обработчике предупреждений PHP; после операции этот обработчик восстанавливается. Ошибки ввода-вывода при завершении сервера отличаются от обычного конца потока. Сбой создания контекста ошибки или вызова обработчика ошибок не останавливает остальные соединения и не прерывает освобождение ресурсов. Неожиданное исключение собственной реализации вызова прикладных обработчиков рассматривается как ошибка приложения, а не нарушение протокола клиентом. `MAX_CONNECTIONS` ограничивает приём соединений, но не гарантирует, что операционная система или `stream_select()` поддерживают такое количество файловых дескрипторов. Непригодные для наблюдения сокеты отклоняются при приёме. Для более масштабных развёртываний предоставьте совместимый селектор или цикл событий.

## Миграция собственных реализаций среды выполнения

Сигнатуры обработчиков приложения и `ConnectionInterface` не изменились. Приложения не должны приводить полученные объекты соединений к конкретному транспортному классу или зависеть от операций с сокетом.

Собственные адаптеры среды выполнения должны учитывать изменения контрактов:

| Контракт | Миграция |
|---|---|
| `MonotonicClockInterface` | Реализуйте `nowMilliseconds(): float` как источник монотонного времени, а не календарного. Стандартная реализация использует `hrtime()`; часы PSR-20 больше не требуются. |
| `TransportConnectionInterface` | Предоставьте стабильный ограниченный объект `application`, `setWriteNotifier()`, `readyToClose`, `failProtocol()`, `awaitingPong` и `pingSent`. Замените `appendBytes()` на `receiveBytes(string $bytes, Closure $onMessage): void`: вызывайте обработчик сообщения синхронно, до обработки следующих кадров. Уведомляйте среду выполнения при появлении данных в пустой очереди или начале закрытия. |
| `PendingConnectionInterface` | Используйте `append(string $bytes)` и `expired(float $nowMs, int $timeoutMs)`; прежние отметки активности и дублирующий адрес удалены из контракта. |
| `ConnectionFactoryInterface` | Принимайте необязательные общие объекты `BufferBudget` для исходящих и входящих данных; освобождайте резерв при отправке, обработке или удалении данных. Сборщик сообщений должен принадлежать одному соединению и сообщать размер накопленных фрагментов через `bufferedBytes`. |
| `WebSocketApplicationInvokerInterface` | Возвращайте логическое значение из `connected()` и `received()`: успешность выполнения обработчика или его сбой; сообщение об ошибке производится по возможности. |
| `SocketSelectorInterface` | Реализуйте `assertSelectable()`, чтобы отклонять непригодные объекты до регистрации наблюдателей. |
| `WebSocketOptionsInterface` | Предоставьте `closeHandshakeTimeoutMs`, `maxTotalOutgoingBufferSize` и `maxTotalIncomingBufferSize`. |

## Граница пакета

Пакет не предоставляет точки входа приложения, не загружает `config/websocket.php` и не регистрирует загрузчик Componenta. Для интеграции с Componenta-приложением используйте `componenta/websocket-app`.

Контракты слоя супервизора, например `WorkerInterface`, `WorkerRegistryInterface` и `WorkerMessageBusInterface`, объявлены здесь, потому что описывают среду выполнения сокет-сервера. Конкретный многопроцессный супервизор относится к приложению или слою развёртывания.

## Проверки при разработке

Установите зависимости для разработки и выполните `vendor/bin/pest`. При включённом Xdebug команда `XDEBUG_MODE=coverage vendor/bin/pest --coverage --coverage-clover=coverage.xml` измеряет покрытие строк исходного кода в процессе тестирования. См. [измерения производительности](benchmarks/README.md), [целевые мутационные проверки](tests/Mutation/README.md) и [нагрузочную проверку TCP](tests/Stress/README.md). Код, выполняемый в дочерних процессах, не входит в этот отчёт о покрытии.
