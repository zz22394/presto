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
package com.facebook.presto.tests.queryinfo;

import com.facebook.presto.tests.CliProcess;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.tests.CliProcess.asCliProcess;
import static com.facebook.presto.tests.CliProcessHelper.waitWithTimeout;
import static com.facebook.presto.tests.JavaProcessUtils.execute;
import static com.facebook.presto.tests.queryinfo.TestClassWithMain.EXPECTED_ARGUMENT;
import static com.facebook.presto.tests.queryinfo.TestClassWithMain.EXPECTED_LINE;
import static com.facebook.presto.tests.queryinfo.TestClassWithMain.PRODUCED_LINE;
import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJavaProcess
{
    @Test
    public void testExecuteJavaProcess()
            throws IOException, InterruptedException
    {
        CliProcess child = asCliProcess(execute(TestClassWithMain.class, newArrayList(EXPECTED_ARGUMENT)));

        child.in.println(EXPECTED_LINE);
        assertThat(child.out.nextLine()).isEqualTo(PRODUCED_LINE);

        assertThat(waitWithTimeout(child)).isEqualTo(0);
    }
}
