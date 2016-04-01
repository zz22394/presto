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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.orc.spiller.OrcSpillerFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.testing.SampledTpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestLocalSpilledQueries extends AbstractTestApproximateQueries
{
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public TestLocalSpilledQueries()
    {
        super(createLocalQueryRunner(), createDefaultSampledSession());
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperties(ImmutableMap.of(SystemSessionProperties.MAX_ENTRIES_BEFORE_SPILL, "8"))
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.addSpillerFactory(new OrcSpillerFactory());
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());
        localQueryRunner.createCatalog(TPCH_SAMPLED_SCHEMA, new SampledTpchConnectorFactory(localQueryRunner.getNodeManager(), 1, 2), ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(AbstractTestQueries.TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties("connector", AbstractTestQueries.TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    private static Session createDefaultSampledSession()
    {
        return testSessionBuilder()
                .setCatalog(TPCH_SAMPLED_SCHEMA)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
    }

    @Test(enabled = false)
    @Override
    public void testGroupByArray()
            throws Exception
    {
        super.testGroupByArray();
    }

    @Test(enabled = false)
    @Override
    public void testGroupByMap()
            throws Exception
    {
        super.testGroupByMap();
    }

    @Test(enabled = false)
    @Override
    public void testGroupByRow()
            throws Exception
    {
        super.testGroupByRow();
    }
}
