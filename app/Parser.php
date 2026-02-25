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

        $offsets = [0];
        $handle = fopen($inputPath, 'r');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle); // advance past the partial line
            $offsets[$i] = ftell($handle);
        }
        fclose($handle);

        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < self::WORKERS; $i++) {
            $tempFile = tempnam(sys_get_temp_dir(), 'parser_');
            $tempFiles[$i] = $tempFile;

            $start = $offsets[$i];
            $end = isset($offsets[$i + 1]) ? $offsets[$i + 1] : $fileSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork process');
            }

            if ($pid === 0) {
                // Child process
                $visits = $this->processChunk($inputPath, $start, $end);
                file_put_contents($tempFile, json_encode($visits));
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
            $visits = json_decode(file_get_contents($tempFile), true);

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

        $json = json_encode($merged, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        $json = str_replace("\n", "\r\n", $json);
        file_put_contents($outputPath, $json);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'r');

        if ($handle === false) {
            throw new Exception("Could not open input file: $inputPath");
        }

        fseek($handle, $start);

        $visits = [];

        while (ftell($handle) < $end && ($line = fgets($handle)) !== false) {
            $trimmed = rtrim($line, "\r\n");

            if ($trimmed === '') {
                continue;
            }

            $commaPos = strpos($trimmed, ',');

            if ($commaPos === false) {
                continue;
            }

            $url  = substr($trimmed, 0, $commaPos);
            $date = substr($trimmed, $commaPos + 1, 10);
            $path = substr($url, strpos($url, '/', 8));

            if ($path === false || $path === null) {
                continue;
            }

            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        return $visits;
    }
}