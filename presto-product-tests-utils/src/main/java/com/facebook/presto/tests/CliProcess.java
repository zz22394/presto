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
import java.util.Scanner;

/**
 * Wrapping class for {@link Process} that keeps textual in/out/err process streams.
 */
public class CliProcess
{
    public final Scanner out;
    public final PrintStream in;
    public final Scanner err;
    public final Process process;

    public static CliProcess asCliProcess(Process process)
    {
        return new CliProcess(
                new Scanner(process.getInputStream()),
                new PrintStream(process.getOutputStream(), true),
                new Scanner(process.getErrorStream()),
                process);
    }

    public CliProcess(Scanner out, PrintStream in, Scanner err, Process process)
    {
        this.out = out;
        this.in = in;
        this.err = err;
        this.process = process;
    }
}
