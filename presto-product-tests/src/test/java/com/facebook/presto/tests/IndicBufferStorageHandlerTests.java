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

import com.facebook.presto.tests.utils.PrestoDDLUtils.Table;
import com.google.common.collect.ImmutableMap;
import com.teradata.test.ProductTest;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.tests.TestGroups.INDIC_BUFFERS;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.facebook.presto.tests.utils.PrestoDDLUtils.createPrestoTable;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.util.Collections.singletonMap;

public class IndicBufferStorageHandlerTests extends ProductTest
{
    private static final String TABLE_NAME = "indic_buffer_table_test";
    private static final Map<String, String> STORAGE_HANDLER_INDIC_BUFFERS = singletonMap("hive.storage_class", "com.teradata.swarm.qg.sh.hive.IndicBuffersStorageHandler");
    private static final String ENCODE_MAX_TEXT_LENGTH_KEY = "hive.serde_parameters.encode-max-text-length";
    private static final int ENCODE_MAX_TEXT_LENGTH = 10;

    @Test(groups = {INDIC_BUFFERS, SMOKE})
    public void shouldCreateIndicBuffersTable()
            throws Exception
    {
        try (Table table = createPrestoTable(TABLE_NAME, "CREATE TABLE %s (i bigint)", STORAGE_HANDLER_INDIC_BUFFERS)) {
            onPresto().executeQuery(String.format("INSERT INTO %s values (42)", table.getNameInDatabase()));
            assertThat(onPresto().executeQuery(String.format("SELECT * FROM %s", table.getNameInDatabase())))
                    .hasColumns(BIGINT)
                    .containsExactly(row(42));
        }
    }

    @Test(groups = INDIC_BUFFERS)
    public void shouldApplyMaxTextLengthRestrictions()
            throws Exception
    {
        Map<String, String> sessionProperties = ImmutableMap.<String, String>builder()
                .putAll(STORAGE_HANDLER_INDIC_BUFFERS)
                .put(ENCODE_MAX_TEXT_LENGTH_KEY, ENCODE_MAX_TEXT_LENGTH + "")
                .build();

        String randomString = generateString(ENCODE_MAX_TEXT_LENGTH * 5);
        String stringExpected = randomString.substring(0, ENCODE_MAX_TEXT_LENGTH);

        try (Table table = createPrestoTable(TABLE_NAME, "CREATE TABLE %s (s varchar)", sessionProperties)) {
            onPresto().executeQuery(String.format("INSERT INTO %s values ('%s')", table.getNameInDatabase(), randomString));
            assertThat(onPresto().executeQuery(String.format("SELECT * FROM %s", table.getNameInDatabase())))
                    .hasColumns(LONGNVARCHAR)
                    .containsExactly(row(stringExpected));
        }
    }

    public static String generateString(int length)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            builder.append((char) ('a' + i % 20));
        }
        return builder.toString();
    }
}
