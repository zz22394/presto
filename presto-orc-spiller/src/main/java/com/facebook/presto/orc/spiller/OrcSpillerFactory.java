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

import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.spi.spiller.SpillerFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.List;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class OrcSpillerFactory implements SpillerFactory
{
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(newFixedThreadPool(4, daemonThreadsNamed("spiller-%s")));

    @Override
    public String getName()
    {
        return "OrcSpillerFactory";
    }

    @Override
    public Spiller create(List<Type> types)
    {
        return new FileSpiller(types, executor);
    }
}
