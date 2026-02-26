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
        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $offsets[$i] = ftell($handle);
        }
        fclose($handle);

        $pids = [];
        $tempFiles = [];

        // Create temp files for each worker
        for ($i = 0; $i < self::WORKERS; $i++) {
            $tempFiles[$i] = sys_get_temp_dir() . '/parser_worker_' . $i . '_' . uniqid() . '.dat';
        }

        // Fork workers
        for ($i = 0; $i < self::WORKERS; $i++) {
            $start = $offsets[$i];
            $end = isset($offsets[$i + 1]) ? $offsets[$i + 1] : $fileSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork process');
            }

            if ($pid === 0) {
                // Child process
                $visits = $this->processChunk($inputPath, $start, $end);
                $data = serialize($visits);
                
                if (file_put_contents($tempFiles[$i], $data) === false) {
                    fwrite(STDERR, "Worker $i: Failed to write temp file\n");
                    exit(1);
                }
                
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Wait for all workers
        foreach ($pids as $workerIndex => $pid) {
            pcntl_waitpid($pid, $status);
            if (pcntl_wexitstatus($status) !== 0) {
                throw new Exception("Worker $workerIndex failed");
            }
        }

        $merged = [];

        // Read and merge results from temp files
        foreach ($tempFiles as $tempFile) {
            if (!file_exists($tempFile)) {
                continue;
            }

            $data = file_get_contents($tempFile);
            $visits = unserialize($data, ['allowed_classes' => false]);

            // Merge
            foreach ($visits as $path => $days) {
                foreach ($days as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            }

            // Clean up temp file
            unlink($tempFile);
        }

        foreach ($merged as &$days) {
            ksort($days, SORT_STRING);
        }
        unset($days);

        $json = json_encode($merged, JSON_PRETTY_PRINT);
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
        $pos = $start;

        while (($line = fgets($handle)) !== false) {
            $pos += strlen($line);
            if ($pos > $end) break;

            $commaPos = strpos($line, ',');
            if ($commaPos === false) continue;

            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        return $visits;
    }
}