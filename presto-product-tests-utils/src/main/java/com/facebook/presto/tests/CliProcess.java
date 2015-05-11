/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Wrapping class for {@link Process} that keeps textual in/out/err process streams.
 */
public class CliProcess
{
    public static final Duration TIMEOUT = Duration.ofSeconds(20);

    public final Scanner out;
    public final PrintStream in;
    public final Scanner err;
    public final Process process;

    public static List<String> trimLines(List<String> lines)
    {
        return lines.stream().map(String::trim).collect(toList());
    }

    public CliProcess(Process process)
    {
        this.out = new Scanner(process.getInputStream());
        this.in = new PrintStream(process.getOutputStream(), true);
        this.err = new Scanner(process.getErrorStream());
        this.process = process;
    }

    public List<String> readRemainingLines()
    {
        List<String> lines = newArrayList();
        while (out.hasNextLine()) {
            lines.add(out.nextLine());
        }
        return lines;
    }

    /**
     * Waits for a process to finish and returns it's status code. If the process
     * fails to finish within given timeout it is killed and {@link RuntimeException} is thrown.
     */
    public int waitWithTimeoutAndKill()
            throws InterruptedException
    {
        if (!process.waitFor(TIMEOUT.toMillis(), MILLISECONDS)) {
            process.destroy();
            throw new RuntimeException("Child process didn't finish within given timeout");
        }
        return process.exitValue();
    }
}
