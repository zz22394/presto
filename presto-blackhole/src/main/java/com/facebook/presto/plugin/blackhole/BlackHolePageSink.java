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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class BlackHolePageSink
        implements ConnectorPageSink
{
    private final ListeningScheduledExecutorService executorService;
    private final long pageProcessingDelayInMillis;

    public BlackHolePageSink(ListeningScheduledExecutorService executorService, Duration pageProcessingDelay)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.pageProcessingDelayInMillis = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null").toMillis();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {
        if (pageProcessingDelayInMillis > 0) {
            return toCompletableFuture(executorService.schedule(() -> null, pageProcessingDelayInMillis, MILLISECONDS));
        }
        return NOT_BLOCKED;
    }

    @Override
    public Collection<Slice> finish()
    {
        return ImmutableList.of();
    }

    @Override
    public void abort() {}
}
