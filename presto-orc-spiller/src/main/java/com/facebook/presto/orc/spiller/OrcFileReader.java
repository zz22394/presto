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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.orc.spiller.OrcFileWriter.createSerializer;
import static com.facebook.presto.orc.spiller.OrcFileWriter.toStorageTypes;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.LongDecimalType.unscaledValueToSlice;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.io.orc.OrcFile.createReader;

public class OrcFileReader implements Closeable
{
    private static final Configuration CONFIGURATION = new Configuration();

    private final RecordReader recordReader;
    //private final StructObjectInspector tableInspector;
    private final StructField[] structFields;

    private final List<Long> columnIds;
    private final List<Type> types;

    private final BlockBuilder[] blockBuilders;
    private final Path path;
    private final List<ObjectInspector> inspectors;
    private final StructObjectInspector structInspector;
    private final int maxRowsPerPage;
    List<String> columnNames;

    public OrcFileReader(List<Long> columnIds, List<Type> columnTypes, Path path, int maxRowsPerPage)
            throws IOException, SerDeException
    {
        this.maxRowsPerPage = maxRowsPerPage;
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");

        Reader reader = createReaderWithFileSystem(path);
        this.recordReader = reader.rows();
        int size = columnIds.size();

        this.columnIds = ImmutableList.copyOf(columnIds);
        this.types = ImmutableList.copyOf(columnTypes);

        this.blockBuilders = new BlockBuilder[size];

        for (int column = 0; column < columnTypes.size(); column++) {
            Type type = columnTypes.get(column);
            blockBuilders[column] = type.createBlockBuilder(new BlockBuilderStatus(), maxRowsPerPage);
        }

        columnNames = ImmutableList.copyOf(transform(columnIds, toStringFunction()));
        List<StorageType> storageTypes = ImmutableList.copyOf(toStorageTypes(columnTypes));
        Iterable<String> hiveTypeNames = storageTypes.stream().map(StorageType::getHiveTypeName).collect(toList());

        Properties properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(columnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        OrcSerde serde = createSerializer(CONFIGURATION, properties);

        structInspector = (StructObjectInspector) serde.getObjectInspector();
        inspectors = storageTypes.stream()
                .map(StorageType::getHiveTypeName)
                .map(TypeInfoUtils::getTypeInfoFromTypeString)
                .map(typeInfo -> (PrimitiveTypeInfo) typeInfo)
                .map(PrimitiveObjectInspectorFactory::getPrimitiveWritableObjectInspector)
                .collect(toList());

        structFields = structInspector.getAllStructFieldRefs().toArray(new StructField[0]);

        this.path = path;
    }

    public boolean hasNext()
    {
        try {
            return recordReader.hasNext();
        }
        catch (IOException e) {
            throw new PrestoException(
                    INTERNAL_ERROR,
                    getErrorMessage(),
                    e);
        }
    }

    public Page getNextPage()
    {
        try {
            for (int column = 0; column < types.size(); column++) {
                Type type = types.get(column);
                blockBuilders[column] = type.createBlockBuilder(new BlockBuilderStatus(), maxRowsPerPage);
            }
            OrcStruct rawRow = null;
            int rows = 0;
            while (recordReader.hasNext()) {
                rows++;
                rawRow = (OrcStruct) recordReader.next(rawRow);

                for (int column = 0; column < types.size(); column++) {
                    Object value = structInspector.getStructFieldData(rawRow, structFields[column]);
                    Type type = types.get(column);
                    BlockBuilder blockBuilder = blockBuilders[column];

                    appendToBlockBuilder(type, ((PrimitiveObjectInspector) inspectors.get(column)).getPrimitiveJavaObject(value), blockBuilder);
                }

                if (rows >= maxRowsPerPage) {
                    break;
                }
            }

            Block[] blocks = new Block[types.size()];
            for (int column = 0; column < types.size(); column++) {
                blocks[column] = blockBuilders[column].build();
            }

            return new Page(rows, blocks);
        }
        catch (IOException e) {
            closeWithSuppression(e);
            throw new PrestoException(
                    INTERNAL_ERROR,
                    getErrorMessage(),
                    e);
        }
    }

    @Override
    public void close()
    {
        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw new PrestoException(INTERNAL_ERROR, getErrorMessage(), e);
        }
    }

    private static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else if (element instanceof SqlDecimal) {
                type.writeSlice(blockBuilder, unscaledValueToSlice(((SqlDecimal) element).getUnscaledValue()));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            type.writeObject(blockBuilder, element);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            throwable.addSuppressed(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnIds)
                .add("types", types)
                .toString();
    }

    private String getErrorMessage()
    {
        return String.format("Failed to read spilled pages from [%s]", path);
    }

    private static Reader createReaderWithFileSystem(Path path)
            throws IOException
    {
        try (FileSystem fileSystem = new SyncingFileSystem(CONFIGURATION)) {
            return createReader(fileSystem, new org.apache.hadoop.fs.Path(path.toUri()));
        }
    }
}
