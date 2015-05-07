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

import java.time.Duration;
import java.util.List;
import java.util.Scanner;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Helper class for interacting with CLI processes.
 */
public final class CliProcessHelper
{
    private static final Duration DEFAULT_WAIT_FOR_TIMEOUT = Duration.ofSeconds(20);

    public static List<String> readRemainingLines(CliProcess cliProcess)
    {
        return readRemainingLines(cliProcess.out);
    }

    public static List<String> readRemainingLines(Scanner scanner)
    {
        List<String> lines = newArrayList();
        while (scanner.hasNextLine()) {
            lines.add(scanner.nextLine());
        }
        return lines;
    }

    public static List<String> trim(List<String> lines)
    {
        return lines.stream().map(String::trim).collect(toList());
    }

    public static int waitWithTimeout(CliProcess cliProcess)
            throws InterruptedException
    {
        return waitWithTimeout(cliProcess.process);
    }

    public static int waitWithTimeout(Process process)
            throws InterruptedException
    {
        return waitWithTimeout(process, DEFAULT_WAIT_FOR_TIMEOUT);
    }

    public static int waitWithTimeout(Process process, Duration duration)
            throws InterruptedException
    {
        checkState(process.waitFor(duration.toMillis(), MILLISECONDS));
        return process.exitValue();
    }

    private CliProcessHelper()
    {
    }
}
