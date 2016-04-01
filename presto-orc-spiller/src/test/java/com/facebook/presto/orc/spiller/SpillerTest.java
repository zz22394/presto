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
package com.facebook.presto.orc.spiller;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;

public class SpillerTest
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
    private final ListeningExecutorService executor = listeningDecorator(newSingleThreadScheduledExecutor());

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (Spiller spiller = new FileSpiller(TYPES, 10, executor)) {
            testSimpleSpiller(spiller);
        }
    }

    @Test
    public void testFileVarbinarySpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col3 = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);

        col1.writeLong(42).closeEntry();
        col2.writeDouble(43.0).closeEntry();
        col3.writeDouble(43.0).writeLong(1).closeEntry();

        Page page = new Page(col1.build(), col2.build(), col3.build());

        try (Spiller spiller = new FileSpiller(types, executor)) {
            testSpiller(types, spiller, ImmutableList.of(page));
        }
    }

    private void testSimpleSpiller(Spiller spiller)
            throws ExecutionException, InterruptedException
    {
        RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 0, 5, 10, 15);
        builder.pageBreak();
        builder.addSequencePage(10, 0, -5, -10, -15);
        List<Page> firstSpill = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 10, 15, 20, 25);
        builder.pageBreak();
        builder.addSequencePage(10, -10, -15, -20, -25);
        List<Page> secondSpill = builder.build();

        testSpiller(TYPES, spiller, firstSpill, secondSpill);
    }

    private void testSpiller(List<Type> types, Spiller spiller, List<Page>... spills)
            throws ExecutionException, InterruptedException
    {
        for (List<Page> spill : spills) {
            spiller.spill(spill.iterator()).get();
        }

        List<Iterator<Page>> actualSpills = spiller.getSpills();

        assertEquals(actualSpills.size(), spills.length);

        for (int i = 0; i < actualSpills.size(); i++) {
            List<Page> spill = ImmutableList.copyOf(actualSpills.get(i));
            List<Page> expectedSpill = spills[i];

            assertEquals(spill.size(), expectedSpill.size());
            for (int j = 0; j < spill.size(); j++) {
                assertPageEquals(types, spill.get(j), expectedSpill.get(j));
            }
        }
    }
}
