<?php

declare(strict_types=1);

if ($argc !== 3) {
    fwrite(STDERR, "Usage: php check-report.php <index.json> <suite.log>\n");
    exit(1);
}

$index = json_decode((string) file_get_contents($argv[1]), true, flags: JSON_THROW_ON_ERROR);
$log = (string) file_get_contents($argv[2]);
$cases = $index['componenta-websocket-server'] ?? null;
if (!is_array($cases)
    || !preg_match('/will run\s+(\d+)\s+test cases against\s+1\s+servers?/i', $log, $match)
    || (int) $match[1] < 100
    || count($cases) !== (int) $match[1]
) {
    fwrite(STDERR, "Autobahn report is incomplete or does not match the planned case count.\n");
    exit(1);
}

$strict = 0;
$nonStrict = 0;
$informational = 0;
$failures = [];
foreach ($cases as $case => $result) {
    $behavior = $result['behavior'] ?? null;
    $closing = $result['behaviorClose'] ?? null;
    if (!in_array($behavior, ['OK', 'NON-STRICT', 'INFORMATIONAL'], true)
        || !in_array($closing, ['OK', 'INFORMATIONAL'], true)
    ) {
        $failures[] = sprintf('%s: behavior=%s close=%s code=%s',
            $case, $behavior ?? 'missing', $closing ?? 'missing', $result['remoteCloseCode'] ?? 'none');
        continue;
    }

    if ($behavior === 'INFORMATIONAL' || $closing === 'INFORMATIONAL') {
        $informational++;
    } elseif ($behavior === 'NON-STRICT') {
        $nonStrict++;
        echo "Non-strict case: {$case}\n";
    } else {
        $strict++;
    }
}

printf("Autobahn: %d cases; %d strict, %d non-strict, %d informational, %d failed.\n",
    count($cases), $strict, $nonStrict, $informational, count($failures));
foreach ($failures as $failure) {
    echo "{$failure}\n";
}

exit($failures === [] ? 0 : 1);
