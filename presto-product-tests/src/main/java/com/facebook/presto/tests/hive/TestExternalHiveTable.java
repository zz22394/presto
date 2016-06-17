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
package com.facebook.presto.tests.hive;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.TableInstance;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;

public class TestExternalHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String EXTERNA_LTABLE_NAME = "target_table";

    public Requirement getRequirements(Configuration configuration)
    {
        return mutableTable(NATION);
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testInsertIntoExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNA_LTABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNA_LTABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery(
                "INSERT INTO hive.default." + EXTERNA_LTABLE_NAME + " SELECT * FROM hive.default." + nation.getNameInDatabase()))
                .failsWithMessage("Cannot write to non-managed Hive table");
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testDeleteFromExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNA_LTABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNA_LTABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNA_LTABLE_NAME))
                .failsWithMessage(" Cannot delete from non-managed Hive table");
    }
}
