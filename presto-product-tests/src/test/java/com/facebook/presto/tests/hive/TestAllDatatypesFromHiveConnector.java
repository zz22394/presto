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
import com.teradata.test.query.QueryType;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_PRESTO_TYPES_ORC;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_PRESTO_TYPES_PARQUET;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_PRESTO_TYPES_RCFILE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_PRESTO_TYPES_TEXTFILE;
import static com.teradata.test.Requirements.compose;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.test.query.QueryExecutor.query;

public class TestAllDatatypesFromHiveConnector
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements()
    {
        return compose(
                immutableTable(ALL_PRESTO_TYPES_TEXTFILE),
                immutableTable(ALL_PRESTO_TYPES_ORC),
                immutableTable(ALL_PRESTO_TYPES_RCFILE),
                immutableTable(ALL_PRESTO_TYPES_PARQUET));
    }

    @Test(groups = "hive_connector")
    public void testSelectAllDatatypesTextFile()
            throws SQLException
    {
        assertProperAllDatatypesSchema("textfile_all_types");

        // we skip c_binary here as it produces unexpected result (see testSelectBinaryColumn)
        assertThat(query("SELECT c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_timestamp, c_date, c_string, c_varchar, c_boolean " +
                "FROM textfile_all_types")).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312, // (double) 123.345f - see limitation #1
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true));
    }

    @Test(groups = {"hive_connector", "quarantine"})
    public void testSelectBinaryColumnTextFile()
            throws SQLException
    {
        assertThat(query("SELECT c_binary FROM textfile_all_types"))
                .containsOnly(row("kot binarny".getBytes()));

        // this test fails we get '��[�v��' instead of 'kot binarny'. This may be hive issue as we
        // get this result even if we connect to hive directly without presto.
    }

    @Test(groups = {"hive_connector", "quarantine"})
    public void testSelectAllDatatypesOrc()
            throws SQLException
    {
        assertProperAllDatatypesSchema("orc_all_types");

        assertThat(query("SELECT c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_timestamp, c_date, c_string, c_boolean " +
                "FROM orc_all_types")).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        (double) 123.345f, // (double) 123.345f - see limitation #1
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        true));
    }

    @Test(groups = {"hive_connector", "quarantine"})
    public void testSelectVarcharColumnForOrc()
            throws SQLException
    {
        assertThat(query("SELECT c_varchar FROM orc_all_types"))
                .containsOnly(row("ala ma kot"));

        // this test fails. It seems like presto implementation of ORC does not support VARCHAR datatype.
        // If VARCHAR column is to be returned from query we get error like below:
        //
        // Query 20150417_002158_00049_b2hmy failed: Error opening Hive split hdfs://hadoop-master:8020/product-test/inline-tables/xorc_all_types/000000_0 (offset=0, length=1317): Unsupported type: VARCHAR
    }

    @Test(groups = "hive_connector")
    public void testSelectAllDatatypesRcfile()
            throws SQLException
    {
        assertProperAllDatatypesSchema("rcfile_all_types");

        assertThat(query("SELECT c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_timestamp, c_date, c_string, c_varchar, c_boolean " +
                "FROM rcfile_all_types")).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345, // for some reason we do not get float/double conversion issue like for text files
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true));
    }

    private void assertProperAllDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "bigint"),
                row("c_smallint", "bigint"),
                row("c_int", "bigint"),
                row("c_bigint", "bigint"),
                row("c_float", "double"),
                row("c_double", "double"),
                row("c_timestamp", "timestamp"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );
    }

    @Test(groups = {"hive_connector", "quarantine"})
    public void testSelectAllDatatypesParquetFile()
            throws SQLException
    {
        // this is stripped from decimal and time columns
        // yet still it does not work through presto, while it work directly from hive
        // fixing would need further investigation.

        assertThat(query("SHOW COLUMNS FROM parquet_all_types", QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "bigint"),
                row("c_smallint", "bigint"),
                row("c_int", "bigint"),
                row("c_bigint", "bigint"),
                row("c_float", "double"),
                row("c_double", "double"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar"),
                row("c_boolean", "boolean")
        );

        assertThat(query("SELECT c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_string, c_varchar, c_boolean " +
                "FROM textfile_all_types")).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312, // (double) 123.345f - see limitation #1
                        234.567,
                        "ala ma kota",
                        "ala ma kot",
                        true));
    }
    // presto limitations referenced above:
    //
    // #1 we have float column with value in 123.345. But presto exposes this column as DOUBLE.
    //    As a result it is processed internally and exposed to the user as java double instead java float,
    //    which have different string representation from what is in hive data file.
    //    For 123.345 we get 123.34500122070312
}
