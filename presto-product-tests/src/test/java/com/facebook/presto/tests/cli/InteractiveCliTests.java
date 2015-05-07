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
package com.facebook.presto.tests.cli;

import com.facebook.presto.cli.Presto;
import com.facebook.presto.tests.CliProcess;
import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import com.teradata.test.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.tests.CliProcess.asCliProcess;
import static com.facebook.presto.tests.CliProcessHelper.readRemainingLines;
import static com.facebook.presto.tests.CliProcessHelper.trim;
import static com.facebook.presto.tests.JavaProcessUtils.execute;
import static com.facebook.presto.tests.TestGroups.CLI;
import static com.teradata.test.fulfillment.hive.tpch.TpchTableDefinitions.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class InteractiveCliTests
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements()
    {
        return new ImmutableTableRequirement(NATION);
    }

    @Test(groups = CLI)
    public void shouldDisplayVersion()
            throws IOException, InterruptedException
    {
        CliProcess presto = launchPrestoCli("--version");
        assertThat(trim(readRemainingLines(presto))).containsExactly("Presto CLI (version unknown)");
    }

    private CliProcess launchPrestoCli(String... arguments)
            throws IOException, InterruptedException
    {
        return launchPrestoCli(Arrays.asList(arguments));
    }

    private CliProcess launchPrestoCli(List<String> arguments)
            throws IOException, InterruptedException
    {
        return asCliProcess(execute(Presto.class, arguments));
    }
}
