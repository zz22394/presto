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

import com.teradata.test.AfterTestWithContext;
import com.teradata.test.BeforeTestWithContext;
import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import com.teradata.test.Requires;
import com.teradata.test.assertions.QueryAssert;
import com.teradata.test.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.SIMPLE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.context.ThreadLocalTestContextHolder.testContextIfSet;
import static com.teradata.test.fulfillment.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.test.query.QueryExecutor.query;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleQueryTest
        extends ProductTest
{
    private static class SimpleTestRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements()
        {
            return new ImmutableTableRequirement(NATION);
        }
    }

    @BeforeTestWithContext
    public void beforeTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @AfterTestWithContext
    public void afterTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectAllFromNation()
    {
        QueryAssert.assertThat(query("select * from nation")).hasRowsCount(25);
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectCountFromNation()
    {
        QueryAssert.assertThat(query("select count(*) from nation"))
                .hasRowsCount(1)
                .contains(row(25));
    }

    @Test(groups = QUARANTINE)
    public void failingTest()
    {
        assertThat(1).isEqualTo(2);
    }

    @Test(enabled = false)
    public void disabledTest()
    {
        assertThat(1).isEqualTo(2);
    }
}
