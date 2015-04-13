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
import com.teradata.test.ProductTest;
import com.teradata.test.configuration.Configuration;
import com.teradata.test.query.JdbcQueryExecutor;
import com.teradata.test.query.QueryExecutor;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.context.ThreadLocalTestContextHolder.testContext;
import static java.sql.JDBCType.BIGINT;

public class IndicBufferStorageHandlerTests extends ProductTest
{
    private static String tableName = "indic_buffer_table_test";

    @Test
    public void testCreateIndicTable() throws SQLException
    {
        Configuration config = testContext().getDependency(Configuration.class).getSubconfiguration("databases.presto");

        try (Connection connection = DriverManager.getConnection(config.getStringMandatory("jdbc_url"),
                                                                 config.getStringMandatory("jdbc_user"),
                                                                 config.getStringMandatory("jdbc_password"));
             QueryExecutor executor = new JdbcQueryExecutor(connection, testContext())) {
            connection.unwrap(PrestoConnection.class).setSessionProperty(
                    "hive.storage_class",
                    "com.teradata.swarm.qg.sh.hive.IndicBuffersStorageHandler");

            executor.executeQuery(String.format("CREATE TABLE %s (i bigint)", tableName));
            try {
                executor.executeQuery(String.format("INSERT INTO %s values (42)", tableName));
                assertThat(executor.executeQuery(String.format("SELECT * FROM %s", tableName)))
                        .hasColumns(BIGINT)
                        .containsExactly(row(42));
            }
            finally {
                executor.executeQuery(String.format("DROP TABLE %s", tableName));
            }
        }
    }
}
