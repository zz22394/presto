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

import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_PRESTO_TYPES_TEXTFILE;
import static com.teradata.test.Requirements.compose;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.test.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.test.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.test.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.test.query.QueryExecutor.query;

public class TestInsertIntoHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String TABLE_NAME = "target_table";

    @Override
    public Requirement getRequirements()
    {
        return compose(
                mutableTable(ALL_PRESTO_TYPES_TEXTFILE, TABLE_NAME, CREATED),
                immutableTable(ALL_PRESTO_TYPES_TEXTFILE));
    }

    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testInsertIntoValuesToHiveTable()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        assertThat(query("INSERT INTO " + tableNameInDatabase + " VALUES(" +
                "127, " +
                "32767, " +
                "2147483647, " +
                "9223372036854775807, " +
                "123.345, " +
                "234.567, " +
                "timestamp '2015-05-10 12:15:35.123', " +
                "date '2015-05-10', " +
                "'ala ma kota', " +
                "'ala ma kota', " +
                "true, " +
                "from_base64('a290IGJpbmFybnk=')" +
                ")")).containsExactly(row(1));

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }

    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testInsertIntoSelectToHiveTable()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        assertThat(query("INSERT INTO " + tableNameInDatabase + " SELECT * from textfile_all_types")).containsExactly(row(1));
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }
}
