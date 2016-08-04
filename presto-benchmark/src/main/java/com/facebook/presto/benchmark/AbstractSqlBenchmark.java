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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.NullOutputOperator;
import com.facebook.presto.testing.NullOutputOperator.NullOutputFactory;
import com.facebook.presto.util.CpuTimer;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    @Language("SQL")
    private final String query;

    protected AbstractSqlBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations,
            @Language("SQL") String query)
    {
        super(localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
        this.query = query;
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        return localQueryRunner.createDrivers(query, new NullOutputFactory(), taskContext);
    }


    protected void runSetUpQuery(String setUpQuery)
    {
        Session session = testSessionBuilder()
                .setSystemProperties(ImmutableMap.of("optimizer.optimize-hash-generation", "true"))
                .build();
        ExecutorService executor = localQueryRunner.getExecutor();
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
        MemoryPool systemMemoryPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));

        TaskContext taskContext = new QueryContext(new QueryId("test"), new DataSize(256, MEGABYTE), memoryPool, systemMemoryPool, executor)
                .addTaskContext(new TaskStateMachine(new TaskId("query", "stage", 0), executor),
                        session,
                        new DataSize(1, MEGABYTE),
                        false,
                        false);

        List<Driver> drivers = localQueryRunner.createDrivers(setUpQuery, new NullOutputFactory(), taskContext);

        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    processed = true;
                }
            }
            done = !processed;
        }
    }
}
