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

package com.facebook.presto.operator.spiller;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.spiller.Spiller;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionedSpiller
{
    private final List<Spiller> spillers;

    public PartitionedSpiller(List<Spiller> spillers)
    {
        requireNonNull(spillers, "spillers is null");

        spillers = ImmutableList.copyOf(spillers);
    }

    public ListenableFuture<?> spill(Iterator<Page> page)
    {
        return null;
    }

    public int getPartitionsCount()
    {
        return spillers.size();
    }

    public List<Iterator<Page>> readSpilledPartition(int partition)
    {
        checkArgument(
                partition < spillers.size() && partition > 0,
                "Expected %d parititon to be between %d and %d",
                partition,
                0,
                spillers.size());

        return spillers.get(partition).getSpills();
    }
}
