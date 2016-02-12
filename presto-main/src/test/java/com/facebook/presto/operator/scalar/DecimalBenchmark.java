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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.DecimalArithmetic;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.type.DecimalOperators;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.LineItem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Iterator;

import static com.facebook.presto.tpch.TpchMetadata.TPCH_GENERATOR_SCALE;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class DecimalBenchmark
{
    static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final int DECIMAL_SCALE = 2;

    @State(Thread)
    public static class BenchmarkState
    {
        @Param({"10000"})
        int numberOfRows;

        @Param({"100", "10", "2", "1"})
        int numberOfColumns;

        Row[] data;

        // accumulators are variables where we store arithmetic operation results
        Slice sliceShortDecimalAccumulator = zeroBigintSlice(); // for Slices it's faster to create local accumulator than to use those predefined ones (JIT creates them on stack?)
        Slice sliceShortDecimalWithFlagAccumulator = zeroBigintSliceWithFlag();
        DecimalArithmetic.MutableDecimal mutableDecimalAccumulator = new DecimalArithmetic.MutableDecimal();
        Slice fastDecimalAccumulator = DecimalArithmetic.decimal();

        Slice result = Slices.allocate(DecimalArithmetic.DECIMAL_SLICE_LENGTH);

        @Setup
        public void setup()
        {
            data = generateData(numberOfRows, numberOfColumns, DECIMAL_SCALE);

            sliceShortDecimalAccumulator = bigintSliceFromLong(0);
            sliceShortDecimalWithFlagAccumulator = bigintSliceWithFlagFromLong(0);
            mutableDecimalAccumulator = new DecimalArithmetic.MutableDecimal();
            fastDecimalAccumulator = DecimalArithmetic.decimal();

            result = Slices.allocate(DecimalArithmetic.DECIMAL_SLICE_LENGTH);
        }
    }

    @Benchmark
    // this simulates using Slice as long accumulator for addition
    public void benchmarkAddExactSliceShortDecimal(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            // simulate a_column + b_column + c_column + ...
            Slice accumulator = zeroBigintSlice(); // its faster to create new Slice accumulator then to zero existing one (?)
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addExactSliceShort(row.getBigintSlice(column), accumulator, accumulator);
            }
            // simulate storing calculation result
            putLongUnsafe(result, getLongUnsafe(accumulator));
        }
    }

    private void addExactSliceShort(Slice left, Slice right, Slice result)
    {
        long leftLong = getLongUnsafe(left);
        long rightLong = getLongUnsafe(right);
        putLongUnsafe(result, Math.addExact(leftLong, rightLong));
    }

    @Benchmark
    // this simulates using Slice as long accumulator for addition
    public void benchmarkAddSliceShortDecimal(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice accumulator = zeroBigintSlice(); // its faster to create new Slice accumulator then to zero existing one (?)
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addSliceShort(row.getBigintSlice(column), accumulator, accumulator);
            }
            putLongUnsafe(result, getLongUnsafe(accumulator));
        }
    }

    private void addSliceShort(Slice left, Slice right, Slice result)
    {
        long leftLong = getLongUnsafe(left);
        long rightLong = getLongUnsafe(right);
        putLongUnsafe(result, leftLong + rightLong);
    }

    @Benchmark
    // add function returns new Slice as a result
    public void benchmarkAddExactSliceShortDecimalNoAccumulator(BenchmarkState state)
    {
        Slice zero = state.sliceShortDecimalAccumulator;
        Slice result = state.result;

        for (Row row : state.data) {
            Slice resultSlice = zero;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultSlice = addExactSliceShortNoAccumulator(row.getBigintSlice(column), resultSlice);
            }
            putLongUnsafe(result, getLongUnsafe(resultSlice));
        }
    }

    private Slice addExactSliceShortNoAccumulator(Slice left, Slice right)
    {
        long leftLong = getLongUnsafe(left);
        long rightLong = getLongUnsafe(right);
        return bigintSliceFromLong(Math.addExact(leftLong, rightLong));
    }

    @Benchmark
    // this simulates using long primitive accumulator for addition
    public void benchmarkAddExactBigInt(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            long resultLong = 0L;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultLong = addExactBigInt(row.getBigint(column), resultLong);
            }
            putLongUnsafe(result, resultLong);
        }
    }

    private long addExactBigInt(long left, long right)
    {
        return Math.addExact(left, right);
    }

    @Benchmark
    // this simulates using long accumulator for addition
    public void benchmarkAddBigInt(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            long resultLong = 0L;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultLong = addBigInt(row.getBigint(column), resultLong);
            }
            putLongUnsafe(result, resultLong);
        }
    }

    private long addBigInt(long left, long right)
    {
        return left + right;
    }

    @Benchmark
    // this simulates using double accumulator for addition
    public void benchmarkAddDouble(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            double resultDouble = 0.0;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultDouble = addDouble(row.getDouble(column), resultDouble);
            }
            putDoubleUnsafe(result, resultDouble);
        }
    }

    private double addDouble(double left, double right)
    {
        return left + right;
    }

    @Benchmark
    // this simulates using Slice as long accumulator for addition.
    // checking condition simulates checking if this is long or short decimal
    public void benchmarkAddExactSliceShortDecimalWithCondition(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice accumulator = zeroBigintSlice();
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addExactSliceShortWithCondition(row.getBigintSlice(column), accumulator, accumulator);
            }
            putLongUnsafe(result, getLongUnsafe(accumulator));
        }
    }

    private void addExactSliceShortWithCondition(Slice left, Slice right, Slice result)
    {
        long leftLong = getLongUnsafe(left);
        long rightLong = getLongUnsafe(right);
        // this emulates checking if decimals are short (represented by longs)
        if ((leftLong & 0x8000000000000000L) == 0 && (rightLong & 0x8000000000000000L) == 0) {
            putLongUnsafe(result, Math.addExact(leftLong, rightLong));
        }
    }

    @Benchmark
    // this simulates using Slice as long accumulator for addition
    public void benchmarkAddSliceShortDecimalWithCondition(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice accumulator = zeroBigintSlice();
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addSliceShortWithCondition(row.getBigintSlice(column), accumulator, accumulator);
            }
            putLongUnsafe(result, getLongUnsafe(accumulator));
        }
    }

    private void addSliceShortWithCondition(Slice left, Slice right, Slice result)
    {
        long leftLong = getLongUnsafe(left);
        long rightLong = getLongUnsafe(right);
        // this emulates checking if decimals are short (represented by longs)
        if ((leftLong & 0x8000000000000000L) == 0 && (rightLong & 0x8000000000000000L) == 0) {
            putLongUnsafe(result, leftLong + rightLong);
        }
    }

    @Benchmark
    // this simulates using Slice as long accumulator for addition.
    // checking flag simulates checking if this is long or short decimal
    public void benchmarkAddExactSliceShortDecimalWithFlag(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice accumulator = zeroBigintSliceWithFlag();
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addExactSliceShortWithFlag(row.getBigintSliceWithFlag(column), accumulator, accumulator);
            }
            putLongUnsafe(result, getLongUnsafe(accumulator));
            putFlagUnsafe(result, getFlagUnsafe(accumulator));
        }
    }

    private void addExactSliceShortWithFlag(Slice left, Slice right, Slice result)
    {
        int flagLeft = getFlagUnsafe(left);
        int flagRight = getFlagUnsafe(right);
        // this emulates checking if decimals are short (represented by longs)
        if (flagLeft == 0 && flagRight == 0) {
            long leftLong = getLongUnsafe(left);
            long rightLong = getLongUnsafe(right);
            putLongUnsafe(result, Math.addExact(leftLong, rightLong));
            putFlagUnsafe(result, 0);
        }
    }

    @Benchmark
    public void benchmarkAddFastStructuredDecimal(BenchmarkState state)
    {
        DecimalArithmetic.MutableDecimal accumulator = state.mutableDecimalAccumulator;
        Slice result = state.result;

        for (Row row : state.data) {
            accumulator.zero();
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addMutableDecimal(row.getFastMutableDecimal(column), accumulator, accumulator);
            }
            putIntUnsafe(result, 0, accumulator.v0);
            putIntUnsafe(result, 1, accumulator.v1);
            putIntUnsafe(result, 2, accumulator.v2);
            putIntUnsafe(result, 3, accumulator.v3);
            putIntUnsafe(result, 4, accumulator.signum);
        }
    }

    private void addMutableDecimal(DecimalArithmetic.MutableDecimal left, DecimalArithmetic.MutableDecimal right, DecimalArithmetic.MutableDecimal result)
    {
        DecimalArithmetic.add(left, right, result);
    }

    @Benchmark
    public void benchmarkAddFastSliceDecimal(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice accumulator = DecimalArithmetic.decimal();
            for (int column = 0; column < state.numberOfColumns; ++column) {
                addFastSliceDecimal(row.getFastDecimal(column), accumulator, accumulator);
            }
            putLongUnsafe(result, 0, getLongUnsafe(accumulator, 0));
            putLongUnsafe(result, 1, getLongUnsafe(accumulator, 1));
            putByteUnsafe(result, 8, getByteUnsafe(accumulator, 8));
        }
    }

    private void addFastSliceDecimal(Slice left, Slice right, Slice result)
    {
        DecimalArithmetic.add(left, right, result);
    }

    @Benchmark
    // add operation returns new fast decimal Slice instance
    public void benchmarkAddFastSliceDecimalNoAccumulator(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice resultDecimal = DecimalArithmetic.ZERO;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultDecimal = addFastSliceDecimalNoAccumulator(row.getFastDecimal(column), resultDecimal);
            }
            putLongUnsafe(result, 0, getLongUnsafe(resultDecimal, 0));
            putLongUnsafe(result, 1, getLongUnsafe(resultDecimal, 1));
            putByteUnsafe(result, 8, getByteUnsafe(resultDecimal, 8));
        }
    }

    private Slice addFastSliceDecimalNoAccumulator(Slice left, Slice right)
    {
        return DecimalArithmetic.add(left, right);
    }

    @Benchmark
    public void benchmarkAddLongDecimal(BenchmarkState state)
    {
        Slice result = state.result;

        for (Row row : state.data) {
            Slice resultDecimal = LongDecimalType.ZERO;
            for (int column = 0; column < state.numberOfColumns; ++column) {
                resultDecimal = DecimalOperators.addLongLongLong(row.getLongDecimal(column), resultDecimal);
            }
            result.setBytes(0, resultDecimal);
        }
    }

    private static int getFlagUnsafe(Slice s)
    {
        return unsafe.getByte(s.getBase(), s.getAddress() + 8);
    }

    private static void putFlagUnsafe(Slice s, int flag)
    {
        unsafe.putByte(s.getBase(), s.getAddress() + 8, (byte) (flag & 0xFF));
    }

    private static int getByteUnsafe(Slice s, int index)
    {
        return unsafe.getByte(s.getBase(), s.getAddress() + index);
    }

    private static void putByteUnsafe(Slice s, int index, int value)
    {
        unsafe.putByte(s.getBase(), s.getAddress() + index, (byte) (value & 0xFF));
    }

    private static long getLongUnsafe(Slice s)
    {
        return unsafe.getLong(s.getBase(), s.getAddress());
    }

    private static long getLongUnsafe(Slice s, int index)
    {
        return unsafe.getLong(s.getBase(), s.getAddress() + index * 8);
    }

    private static double getDoubleUnsafe(Slice s)
    {
        return unsafe.getDouble(s.getBase(), s.getAddress());
    }

    private static void putDoubleUnsafe(Slice s, double value)
    {
        unsafe.putDouble(s.getBase(), s.getAddress(), value);
    }

    private static void putLongUnsafe(Slice s, long value)
    {
        unsafe.putLong(s.getBase(), s.getAddress(), value);
    }

    private static void putLongUnsafe(Slice s, int index, long value)
    {
        unsafe.putLong(s.getBase(), s.getAddress() + index * 8, value);
    }

    private static void putIntUnsafe(Slice s, int index, int value)
    {
        unsafe.putInt(s.getBase(), s.getAddress() + index * 4, value);
    }

    private static class Row
    {
        final Slice[] bigintColumns;
        final Slice[] bigintWithFlagColumns;
        final Slice[] fastDecimalColumns;
        final Slice[] longDecimalColumns;
        final Slice[] doubleColumns;

        Row(Slice[] bigintColumns, Slice[] bigintWithFlagColumns, Slice[] fastDecimalColumns, Slice[] longDecimalColumns, Slice[] doubleColumns)
        {
            this.bigintColumns = bigintColumns;
            this.bigintWithFlagColumns = bigintWithFlagColumns;
            this.fastDecimalColumns = fastDecimalColumns;
            this.longDecimalColumns = longDecimalColumns;
            this.doubleColumns = doubleColumns;
        }

        long getBigint(int columnIndex)
        {
            return getLongUnsafe(bigintColumns[columnIndex]);
        }

        Slice getBigintSlice(int columnIndex)
        {
            Slice slice = bigintColumns[columnIndex];
            return slice.slice(0, 8);
        }

        Slice getBigintSliceWithFlag(int columnIndex)
        {
            Slice slice = bigintWithFlagColumns[columnIndex];
            return slice.slice(0, 9);
        }

        Slice getFastDecimal(int columnIndex)
        {
            Slice slice = fastDecimalColumns[columnIndex];
            return slice.slice(0, DecimalArithmetic.DECIMAL_SLICE_LENGTH);
        }

        DecimalArithmetic.MutableDecimal getFastMutableDecimal(int columnIndex)
        {
            return DecimalArithmetic.mutableDecimal(getBigint(columnIndex));
        }

        Slice getLongDecimal(int columnIndex)
        {
            Slice slice = longDecimalColumns[columnIndex];
            return slice.slice(0, LongDecimalType.LONG_DECIMAL_LENGTH);
        }

        double getDouble(int columnIndex)
        {
            Slice slice = doubleColumns[columnIndex];
            return getDoubleUnsafe(slice);
        }
    }

    private static Row[] generateData(int numberOfRows, int numberOfColumns, int scale)
    {
        long rescale = ShortDecimalType.tenToNth(scale - TPCH_GENERATOR_SCALE);
        Iterator<LineItem> lineItem = LINE_ITEM.createGenerator(1d, 1, 1).iterator();
        Row[] rows = new Row[numberOfRows];

        for (int row = 0; row < numberOfRows; ++row) {
            Slice[] bigintColumns = new Slice[numberOfColumns];
            Slice[] bigintWithFlagColumns = new Slice[numberOfColumns];
            Slice[] fastDecimalColumns = new Slice[numberOfColumns];
            Slice[] longDecimalColumns = new Slice[numberOfColumns];
            Slice[] doubleColumns = new Slice[numberOfColumns];
            for (int column = 0; column < numberOfColumns; ++column) {
                long value = lineItem.next().getExtendedPriceInCents() * rescale;
                bigintColumns[column] = bigintSliceFromLong(value);
                bigintWithFlagColumns[column] = bigintSliceWithFlagFromLong(value);
                fastDecimalColumns[column] = DecimalArithmetic.decimal(value);
                longDecimalColumns[column] = LongDecimalType.unscaledValueToSlice(value);
                doubleColumns[column] = sliceFromDouble(value);
            }
            rows[row] = new Row(bigintColumns, bigintWithFlagColumns, fastDecimalColumns, longDecimalColumns, doubleColumns);
        }
        return rows;
    }

    private static Slice zeroBigintSlice()
    {
        return Slices.allocate(8);
    }

    private static Slice zeroBigintSliceWithFlag()
    {
        return Slices.allocate(9);
    }

    private static Slice bigintSliceFromLong(long value)
    {
        Slice result = Slices.allocate(8);
        result.setLong(0, value);
        return result;
    }

    private static Slice sliceFromDouble(double value)
    {
        Slice result = Slices.allocate(8);
        result.setDouble(0, value);
        return result;
    }

    private static Slice bigintSliceWithFlagFromLong(long value)
    {
        Slice result = Slices.allocate(9);
        result.setLong(0, value);
        return result;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + DecimalBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
