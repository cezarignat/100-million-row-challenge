<?php

namespace App;

use Exception;

final class Parser
{
    private const WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKERS);

        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < self::WORKERS; $i++) {
            $tempFile = tempnam(sys_get_temp_dir(), 'parser_');
            $tempFiles[$i] = $tempFile;

            $offset = $i * $chunkSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork process');
            }

            if ($pid === 0) {
                // Child process
                $visits = $this->processChunk($inputPath, $offset, $chunkSize, $i === 0);
                file_put_contents($tempFile, serialize($visits));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];

        foreach ($tempFiles as $tempFile) {
            $handle = fopen($tempFile, 'r');
            $visits = unserialize(stream_get_contents($handle));
            fclose($handle);

            unlink($tempFile);

            foreach ($visits as $path => $days) {
                foreach ($days as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            }
        }

        foreach ($merged as &$days) {
            ksort($days, SORT_STRING);
        }
        unset($days);

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
    }

    private function processChunk(string $inputPath, int $offset, int $chunkSize, bool $isFirst): array
    {
        $handle = fopen($inputPath, 'r');

        if ($handle === false) {
            throw new Exception("Could not open input file: $inputPath");
        }

        fseek($handle, $offset);

        if (!$isFirst) {
            fgets($handle);
        }

        $visits = [];
        $bytesRead = 0;

        while ($bytesRead <= $chunkSize && ($line = fgets($handle)) !== false) {
            $bytesRead += strlen($line);

            if ($line === '') {
                continue;
            }

            $commaPos = strrpos($line, ',');

            if ($commaPos === false) {
                continue;
            }

            $url  = substr($line, 0, $commaPos);
            $date = substr($line, $commaPos + 1, 10);
            $path = parse_url($url, PHP_URL_PATH);

            if ($path === false || $path === null) {
                continue;
            }

            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        return $visits;
    }
}