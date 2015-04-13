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

import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import com.teradata.test.Requires;
import com.teradata.test.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.fulfillment.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.test.query.QueryExecutor.query;
import static java.lang.String.format;

public class CreateTableTests extends ProductTest
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

    @Test(groups = "create_table")
    @Requires(SimpleTestRequirements.class)
    public void testCreateTableAsSelect() throws SQLException
    {
        String tableName = "create_table_as_select";
        query(format("CREATE TABLE %s AS SELECT * FROM nation", tableName));
        try {
            assertThat(query(format("SELECT * FROM %s", tableName))).hasRowsCount(25);
        }
        finally {
            query(format("DROP TABLE %s", tableName));
        }
    }

    @Test(groups = "create_table")
    @Requires(SimpleTestRequirements.class)
    public void testCreateTableAsEmptySelect() throws SQLException
    {
        String tableName = "create_table_as_empty_select";
        query(format("CREATE TABLE %s AS SELECT * FROM nation", tableName));
        try {
            assertThat(query(format("SELECT * FROM %s WHERE 0 is NULL", tableName))).hasRowsCount(0);
        }
        finally {
            query(format("DROP TABLE %s", tableName));
        }
    }
}
