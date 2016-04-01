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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import org.apache.hadoop.hive.serde2.SerDeException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class FileSpiller implements Spiller
{
    private static final Path SPILL_PATH = Paths.get("/tmp/spills");

    static {
        SPILL_PATH.toFile().mkdirs();
    }

    private final List<Type> columns;
    private final int maxRowsPerPage;
    private final List<Long> columnIds;

    private final Path targetDirectory;
    private final Closer closer = Closer.create();

    private int spillsCount = 0;
    private final ListeningExecutorService executor;

    public FileSpiller(List<Type> columns, ListeningExecutorService executor)
    {
        this(columns, 1024, executor);
    }

    public FileSpiller(List<Type> columns, int maxRowsPerPage, ListeningExecutorService executor)
    {
        this.maxRowsPerPage = maxRowsPerPage;
        this.columns = requireNonNull(columns, "columns is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.columnIds = LongStream.range(0, columns.size()).boxed().collect(toList());
        try {
            this.targetDirectory = Files.createTempDirectory(SPILL_PATH, "presto-spill");
        }
        catch (IOException e) {
            throw new PrestoException(INTERNAL_ERROR, "Failed to create spill directory", e);
        }
    }

    @Override
    public CompletableFuture<?> spill(Iterator<Page> pageIterator)
    {
        Path spillPath = getPath(spillsCount++);

        return MoreFutures.toCompletableFuture(executor.submit(
                () -> {
                    try {
                        writePages(pageIterator, spillPath);
                    }
                    catch (IOException e) {
                        throw new PrestoException(INTERNAL_ERROR, "Failed to spill pages", e);
                    }
                }
        ));
    }

    private void writePages(Iterator<Page> pageIterator, Path spillPath)
            throws IOException
    {
        File file = spillPath.toFile();
        try (OrcFileWriter fileWriter = new OrcFileWriter(columnIds, columns, file)) {
            fileWriter.appendPages(pageIterator);
        }
    }

    @Override
    public List<Iterator<Page>> getSpills()
    {
        try {
            ImmutableList.Builder<Iterator<Page>> spills = ImmutableList.builder();
            for (int spillNumber = 0; spillNumber < spillsCount; spillNumber++) {
                Path spillPath = getPath(spillNumber);
                spills.add(readPages(spillPath));
            }

            return spills.build();
        }
        catch (IOException | SerDeException e) {
            throw new PrestoException(INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    private Iterator<Page> readPages(Path spillPath)
            throws IOException, SerDeException
    {
        OrcFileReader reader = new OrcFileReader(columnIds, columns, spillPath, maxRowsPerPage);
        closer.register(reader);

        return new Iterator<Page>() {
            @Override
            public boolean hasNext()
            {
                return reader.hasNext();
            }

            @Override
            public Page next()
            {
                return reader.getNextPage();
            }
        };
    }

    @Override
    public void close()
    {
        try (Stream<Path> list = Files.list(targetDirectory)) {
            closer.close();
            for (Path path : list.collect(toList())) {
                Files.delete(path);
            }
            Files.delete(targetDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(
                    INTERNAL_ERROR,
                    String.format("Failed to delete directory [%s]", targetDirectory),
                    e);
        }
    }

    private Path getPath(int spillNumber)
    {
        return Paths.get(targetDirectory.toAbsolutePath().toString(), String.format("%d.orc", spillNumber));
    }
}
