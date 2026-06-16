<?php

declare(strict_types=1);

function webSocketNamespaceRoot(): string
{
    return (string) getcwd() . DIRECTORY_SEPARATOR . 'src' . DIRECTORY_SEPARATOR . 'websocket-server'
        . DIRECTORY_SEPARATOR . 'src';
}

function webSocketNamespaceFromFile(string $file): string
{
    $contents = (string) file_get_contents($file);

    if (!preg_match('/^namespace\s+([^;]+);/m', $contents, $matches)) {
        throw new RuntimeException(sprintf('File "%s" does not declare a namespace.', $file));
    }

    return $matches[1];
}

describe('WebSocket namespace layout', function () {
    it('keeps classes in layer namespaces instead of the flat WebSocket namespace', function () {
        $root = webSocketNamespaceRoot();
        $rootFiles = array_values(array_filter(
            scandir($root) ?: [],
            static fn(string $entry): bool => is_file($root . DIRECTORY_SEPARATOR . $entry),
        ));
        sort($rootFiles);

        expect($rootFiles)->toBe(['ConfigProvider.php']);

        $expected = [
            'Application' => 'Componenta\\WebSocket\\Application',
            'Application/Error' => 'Componenta\\WebSocket\\Application\\Error',
            'Config' => 'Componenta\\WebSocket\\Config',
            'Connection' => 'Componenta\\WebSocket\\Connection',
            'Exception' => 'Componenta\\WebSocket\\Exception',
            'Loop' => 'Componenta\\WebSocket\\Loop',
            'Protocol' => 'Componenta\\WebSocket\\Protocol',
            'Protocol/Handshake' => 'Componenta\\WebSocket\\Protocol\\Handshake',
            'Socket' => 'Componenta\\WebSocket\\Socket',
            'Supervisor' => 'Componenta\\WebSocket\\Supervisor',
            'Transport' => 'Componenta\\WebSocket\\Transport',
        ];
        $violations = [];
        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root));

        foreach ($iterator as $file) {
            if (!$file instanceof SplFileInfo || !$file->isFile() || $file->getExtension() !== 'php') {
                continue;
            }

            $relative = str_replace('\\', '/', substr($file->getPathname(), strlen($root) + 1));

            if ($relative === 'ConfigProvider.php') {
                $namespace = webSocketNamespaceFromFile($file->getPathname());

                if ($namespace !== 'Componenta\\WebSocket') {
                    $violations[$relative] = $namespace;
                }

                continue;
            }

            $parts = explode('/', $relative);
            $key = $parts[0];

            if ($key === 'Application' && ($parts[1] ?? null) === 'Error') {
                $key = 'Application/Error';
            } elseif ($key === 'Protocol' && ($parts[1] ?? null) === 'Handshake') {
                $key = 'Protocol/Handshake';
            }

            $namespace = webSocketNamespaceFromFile($file->getPathname());

            if (($expected[$key] ?? null) !== $namespace) {
                $violations[$relative] = $namespace;
            }
        }

        expect($violations)->toBeEmpty();
    });

    it('does not import app WebSocket internals in the server package', function () {
        $root = (string) getcwd();
        $paths = [
            $root . DIRECTORY_SEPARATOR . 'src' . DIRECTORY_SEPARATOR . 'websocket-server' . DIRECTORY_SEPARATOR . 'src',
            $root . DIRECTORY_SEPARATOR . 'src' . DIRECTORY_SEPARATOR . 'websocket-server' . DIRECTORY_SEPARATOR . 'tests',
        ];
        $violations = [];

        foreach ($paths as $path) {
            if (!is_dir($path)) {
                continue;
            }

            $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($path));

            foreach ($iterator as $file) {
                if (!$file instanceof SplFileInfo || !$file->isFile() || $file->getExtension() !== 'php') {
                    continue;
                }

                $contents = (string) file_get_contents($file->getPathname());

                if (preg_match_all('/Componenta\\\\App\\\\WebSocket/', $contents, $matches) > 0) {
                    $violations[$file->getPathname()] = $matches[0];
                }
            }
        }

        expect($violations)->toBeEmpty();
    });
});
