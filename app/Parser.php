<?php

namespace App;

use Exception;

final class Parser
{
    private const WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKERS);

        $offsets = [0];
        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
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
            $visits = unserialize(
                file_get_contents($tempFile),
                ['allowed_classes' => false]
            );

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

        $json = json_encode($merged, JSON_PRETTY_PRINT);
        $json = str_replace("\n", "\r\n", $json);
        file_put_contents($outputPath, $json);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');


        if ($handle === false) {
            throw new Exception("Could not open input file: $inputPath");
        }

        fseek($handle, $start);

        $visits = [];

        $bytesRead = 0;
        $limit = $end - $start;

        while (($line = fgets($handle)) !== false) {
            if (ftell($handle) > $end) {
                break;
            }


            $commaPos = strpos($line, ',');

            if ($commaPos === false) {
                continue;
            }

            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            if ($path === false || $path === null) {
                continue;
            }

            //echo $date . PHP_EOL; die();


            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        return $visits;
    }
}