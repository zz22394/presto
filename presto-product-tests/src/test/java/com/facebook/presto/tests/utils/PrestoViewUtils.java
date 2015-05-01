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
package com.facebook.presto.tests.utils;

import com.teradata.test.query.QueryExecutionException;
import org.apache.commons.lang3.RandomStringUtils;

import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.query.QueryExecutor.query;
import static com.teradata.test.query.QueryType.UPDATE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public final class PrestoViewUtils
{
    public static void withView(String createViewDDL, ViewTestCase viewTestCase)
    {
        String viewName = generateViewName();
        tryDropView(viewName);
        try {
            createView(createViewDDL, viewName);

            viewTestCase.test(viewName);
        }
        finally {
            tryDropView(viewName);
        }
    }

    private static String generateViewName()
    {
        return "TEST_VIEW_" + RandomStringUtils.randomAlphanumeric(4);
    }

    private static void createView(String createViewDDL, String viewName)
    {
        assertThat(query(format(createViewDDL, viewName), UPDATE))
                .hasRowsCount(1);
    }

    private static void tryDropView(String viewName)
    {
        try {
            assertThat(query(format("DROP VIEW %s", viewName), UPDATE))
                    .hasRowsCount(1);
        }
        catch (QueryExecutionException e) {
            // view might not exist
            assertThat(e.getMessage())
                    .contains("Query failed")
                    .contains("does not exist");
        }
    }

    private PrestoViewUtils()
    {
    }

    public interface ViewTestCase
    {
        void test(String viewName);
    }
}
