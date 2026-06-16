<?php

declare(strict_types=1);

namespace Componenta\WebSocket\Protocol;

final class CloseCode
{
    public const int NORMAL = 1000;
    public const int GOING_AWAY = 1001;
    public const int PROTOCOL_ERROR = 1002;
    public const int ABNORMAL = 1006;
    public const int INVALID_PAYLOAD_DATA = 1007;
    public const int PAYLOAD_TOO_LARGE = 1009;
    public const int INTERNAL_ERROR = 1011;
}
