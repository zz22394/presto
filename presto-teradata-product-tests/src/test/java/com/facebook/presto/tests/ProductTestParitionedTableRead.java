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

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.tests.utils.QueryExecutors;
import com.google.inject.Inject;
import com.teradata.test.BeforeTestWithContext;
import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import com.teradata.test.fulfillment.hive.HiveTableDefinition;
import com.teradata.test.fulfillment.table.ImmutableTableRequirement;
import com.teradata.test.fulfillment.table.MutableTableRequirement;
import com.teradata.test.fulfillment.table.MutableTablesState;
import com.teradata.test.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.Statement;

import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.test.Requirements.compose;
import static com.teradata.test.fulfillment.hive.tpch.TpchTableDefinitions.NATION;
import static org.testng.Assert.assertEquals;

public class ProductTestParitionedTableRead extends ProductTest implements RequirementsProvider
{
    @Inject
    private MutableTablesState mutableTablesState;

    private static final String PARTITIONED_NATION_NAME = "partitioned_nation_read_test";
    private static final String TARGET_NATION_NAME = "target_nation_test";

    private static final HiveTableDefinition PARTITIONED_NATION =
            HiveTableDefinition.builder()
                    .setName(PARTITIONED_NATION_NAME)
                    .setCreateTableDDLTemplate("" +
                            "CREATE EXTERNAL TABLE %NAME%(" +
                            "   p_nationkey     INT," +
                            "   p_name          STRING," +
                            "   p_comment       STRING) " +
                            "PARTITIONED BY (p_regionkey INT)" +
                            "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                            "LOCATION '%LOCATION%'")
                    .setNoData()
                    .build();

    @Override
    public Requirement getRequirements()
    {
        return compose(
                new ImmutableTableRequirement(NATION),
                new MutableTableRequirement(PARTITIONED_NATION),
                new MutableTableRequirement(HiveTableDefinition.like(PARTITIONED_NATION)
                                                               .setName(TARGET_NATION_NAME)
                                                               .build()));
    }

    @BeforeTestWithContext
    public void beforeTest()
    {
        String insertQueryFormat = new StringBuilder()
                .append("INSERT INTO TABLE ").append(mutableTablesState.get(PARTITIONED_NATION_NAME).getNameInDatabase()).append(" PARTITION (p_regionkey=%d) ")
                .append("SELECT n_nationkey, n_name, n_comment FROM ").append(NATION.getName()).append(" WHERE n_regionkey=%d")
                .toString();

        onHive().executeQuery(String.format(insertQueryFormat, 1, 1));
        onHive().executeQuery(String.format(insertQueryFormat, 2, 2));
        onHive().executeQuery(String.format(insertQueryFormat, 3, 3));
    }

    @Test
    public void selectFromPartitionedNation() throws Exception
    {
        // read all data
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_nationkey < 40", 5);

        // read no partitions
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey = 42", 1);

        // read one partition
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey = 2 AND p_nationkey < 40", 3);
        // read two partitions
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey = 2 AND p_nationkey < 40 or p_regionkey = 3", 4);
        // read all (three) partitions
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey = 2 OR p_nationkey < 40", 5);

        // range read two partitions
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey <= 2", 4);
        testQuerySplitsNumber("INSERT INTO %s SELECT * FROM %s WHERE p_regionkey <= 1 OR p_regionkey >= 3", 4);
    }

    private void testQuerySplitsNumber(String query, int expectedSplitsNumber) throws Exception
    {
        String partitionedNation = mutableTablesState.get(PARTITIONED_NATION_NAME).getNameInDatabase();
        String targetNation = mutableTablesState.get(TARGET_NATION_NAME).getNameInDatabase();

        String queryId;
        try (PrestoConnection prestoConnection = QueryExecutors.createPrestoConnection()) {
            queryId = executeAndGetQueryId(
                    prestoConnection,
                    String.format(query,
                            targetNation,
                            partitionedNation));
        }

        long splitsNumber = getSplitsNumber(queryId);
        assertEquals(splitsNumber, expectedSplitsNumber);
    }

    private String executeAndGetQueryId(PrestoConnection prestoConnection, String query) throws Exception
    {
        try (Statement statement = prestoConnection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(query)) {
                PrestoResultSet prestoResultSet = resultSet.unwrap(PrestoResultSet.class);
                while (prestoResultSet.next()) {
                    // read all query's output to finish query
                    continue;
                }
                return prestoResultSet.getQueryId();
            }
        }
    }

    private long getSplitsNumber(String queryId)
    {
        String queryBytes = String.format(
                "SELECT splits FROM system.runtime.tasks WHERE query_id='%s'",
                queryId);

        QueryResult queryResult = onPresto().executeQuery(queryBytes);
        return queryResult.column(1).stream().mapToLong(t -> (long) t).sum();
    }
}
