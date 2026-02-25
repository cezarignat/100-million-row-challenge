<?php

namespace App;

use Exception;

final class Parser
{
    private const WORKERS = 8;
    private const SHM_SIZE = 128 * 1024 * 1024; // 128 MB per worker

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
        $shmIds = [];

        for ($i = 0; $i < self::WORKERS; $i++) {

            $key = ftok(__FILE__, chr(65 + $i));
            
            $shmId = shmop_open($key, 'c', 0600, self::SHM_SIZE);
            if ($shmId === false) {
                throw new Exception('Failed to create shared memory');
            }

            $shmIds[$i] = $shmId;

            $start = $offsets[$i];
            $end = isset($offsets[$i + 1]) ? $offsets[$i + 1] : $fileSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork process');
            }

            if ($pid === 0) {
                $visits = $this->processChunk($inputPath, $start, $end);
                $data = serialize($visits);
                $len  = strlen($data);

                if ($len + 8 > self::SHM_SIZE) {
                    fwrite(STDERR, "Shared memory too small\n");
                    exit(1);
                }

                shmop_write($shmIds[$i], pack('J', $len), 0);
                shmop_write($shmIds[$i], $data, 8);
                exit(0);
            }


            $pids[$i] = $pid;
        }

        foreach ($pids as $workerIndex => $pid) {
            pcntl_waitpid($pid, $status);
            if (pcntl_wexitstatus($status) !== 0) {
                throw new Exception("Worker $workerIndex failed");
            }
        }

        $merged = [];

        foreach ($shmIds as $shmId) {
            // read length
            $lenData = shmop_read($shmId, 0, 8);
            $len = unpack('J', $lenData)[1];

            if ($len === 0) {
                shmop_delete($shmId);
                continue;
            }

            // read payload
            $data = shmop_read($shmId, 8, $len);

            $visits = unserialize($data, ['allowed_classes' => false]);

            // merge into $merged
            foreach ($visits as $path => $days) {
                foreach ($days as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            }

            shmop_delete($shmId);
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
        $buffer = '';
        $pos = $start;

        while (true) {
            $chunk = fread($handle, 65536);
            if ($chunk === false || $chunk === '') break;

            $buffer .= $chunk;
            $newlinePos = strrpos($buffer, "\n");
            if ($newlinePos === false) continue;

            $block = substr($buffer, 0, $newlinePos + 1);
            $buffer = substr($buffer, $newlinePos + 1);

            $pos += strlen($block);
            $stop = $pos > $end;

            foreach (explode("\n", $block) as $line) {
                $commaPos = strpos($line, ',');
                if ($commaPos === false) continue;
                $path = substr($line, 19, $commaPos - 19);
                $date = substr($line, $commaPos + 1, 10);
                $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
            }

            if ($stop) break;
        }

        fclose($handle);

        return $visits;
    }
}