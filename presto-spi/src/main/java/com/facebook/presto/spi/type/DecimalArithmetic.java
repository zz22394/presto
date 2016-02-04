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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.math.BigInteger;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.math.BigInteger.TEN;

public final class DecimalArithmetic
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

    public static final int DECIMAL_SLICE_LENGTH = 2 * SIZE_OF_LONG + 1;
    public static final int MAX_PRECISION = 38;

    public static final Slice ZERO = decimal(0L);

    private static final int DECIMAL_BYTES_LENGTH = DECIMAL_SLICE_LENGTH - 1;
    private static final int SIGNUM_BYTE_INDEX = 2 * SIZE_OF_LONG;
    private static final long LONG_MASK = 0xFFFFFFFFL;
    private static final int TEN_TO_NTH_TABLE_LENGTH = 38;

    private static final Slice[] TEN_TO_NTH = new Slice[TEN_TO_NTH_TABLE_LENGTH];

    static {
        for (int i = 0; i < TEN_TO_NTH.length; ++i) {
            TEN_TO_NTH[i] = decimal(TEN.pow(i));
        }
    }

    public static class MutableDecimal
    {
        public int signum;
        public int v0;
        public int v1;
        public int v2;
        public int v3;

        public MutableDecimal()
        {
        }

        public MutableDecimal(int signum, int v0, int v1, int v2, int v3)
        {
            this.signum = signum;
            this.v0 = v0;
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        public void set(MutableDecimal other)
        {
            this.signum = other.signum;
            this.v0 = other.v0;
            this.v1 = other.v1;
            this.v2 = other.v2;
            this.v3 = other.v3;
        }

        public void zero()
        {
            signum = v0 = v1 = v2 = v3 = 0;
        }
    }

    public static Slice tenToNth(int n)
    {
        return TEN_TO_NTH[n];
    }

    public static Slice decimal()
    {
        return Slices.allocate(DECIMAL_SLICE_LENGTH);
    }

    public static Slice decimal(Slice decimal)
    {
        return Slices.copyOf(decimal);
    }

    public static Slice decimal(String unscaledValue)
    {
        return decimal(new BigInteger(unscaledValue));
    }

    public static Slice decimal(BigInteger unscaledValue)
    {
        byte[] bytes = unscaledValue.abs().toByteArray();

        if (bytes.length > DECIMAL_BYTES_LENGTH) {
            throwOverflowException();
        }

        // convert to little-endian order
        reverse(bytes);

        Slice decimal = Slices.allocate(DECIMAL_SLICE_LENGTH);
        decimal.setBytes(0, bytes);
        decimal.setByte(SIGNUM_BYTE_INDEX, unscaledValue.signum());

        throwIfOverflows(decimal);

        return decimal;
    }

    public static Slice decimal(long unscaledValue)
    {
        byte[] bytes = new byte[DECIMAL_SLICE_LENGTH];
        if (unscaledValue < 0) {
            unscaledValue = -unscaledValue;
            bytes[SIGNUM_BYTE_INDEX] = -1;
        }
        else if (unscaledValue > 0) {
            bytes[SIGNUM_BYTE_INDEX] = 1;
        }
        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            bytes[i] = (byte) ((unscaledValue >> i * 8) & 0xff);
        }
        return Slices.wrappedBuffer(bytes);
    }

    public static MutableDecimal mutableDecimal(long unscaledValue)
    {
        MutableDecimal result = new MutableDecimal();
        if (unscaledValue < 0) {
            unscaledValue = -unscaledValue;
            result.signum = -1;
        }
        else if (unscaledValue > 0) {
            result.signum = 1;
        }
        result.v0 = (int) (unscaledValue >> 24);
        result.v1 = (int) (unscaledValue >> 16);
        result.v2 = (int) (unscaledValue >> 8);
        result.v3 = (int) unscaledValue;
        return result;
    }

    public static BigInteger decimalToBigInteger(Slice decimal)
    {
        byte[] bytes = decimal.getBytes(0, DECIMAL_BYTES_LENGTH);
        // convert to big-endian order
        reverse(bytes);
        return new BigInteger(getSignum(decimal), bytes);
    }

    public static Slice add(Slice left, Slice right)
    {
        Slice result = decimal();
        add(left, right, result);
        return result;
    }

    public static void add(Slice left, Slice right, Slice result)
    {
        int leftSignum = getSignum(left);
        int rightSignum = getSignum(right);

        if (leftSignum == 0) {
            result.setBytes(0, right);
            return;
        }

        if (leftSignum == rightSignum) {
            addUnsigned(left, right, result);
            setSignum(result, leftSignum);
        }
        else {
            int cmp = differenceUnsigned(left, right, result);
            if (cmp > 0) {
                setSignum(result, leftSignum);
            }
            else if (cmp == 0) {
                setSignum(result, 0);
            }
            else {
                setSignum(result, rightSignum);
            }
        }

        throwIfOverflows(result);
    }

    public static void add(MutableDecimal left, MutableDecimal right, MutableDecimal result)
    {
        if (left.signum == 0) {
            result.set(right);
            return;
        }

        if (left.signum == right.signum) {
            addUnsigned(left, right, result);
            result.signum = left.signum;
        }
        else {
            int cmp = differenceUnsigned(left, right, result);
            if (cmp > 0) {
                result.signum = left.signum;
            }
            else if (cmp == 0) {
                result.signum = 0;
            }
            else {
                result.signum = right.signum;
            }
        }

        throwIfOverflows(result);
    }

    public static Slice multiply(Slice left, Slice right)
    {
        Slice result = decimal();
        multiply(left, right, result);
        return result;
    }

    public static void multiply(Slice left, Slice right, Slice result)
    {
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        // the combinations below definitely result in overflow
        if ((r3 != 0 && (l3 != 0 || l2 != 0 || l1 != 0))
                || (r2 != 0 && (l3 != 0 || l2 != 0))
                || (r1 != 0 && l3 != 0)) {
            throwOverflowException();
        }

        long product;

        product = (r0 & LONG_MASK) * (l0 & LONG_MASK);
        int z0 = (int) product;

        product = (r0 & LONG_MASK) * (l1 & LONG_MASK)
                + (r1 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z1 = (int) product;

        product = (r0 & LONG_MASK) * (l2 & LONG_MASK)
                + (r1 & LONG_MASK) * (l1 & LONG_MASK)
                + (r2 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z2 = (int) product;

        // v[3]
        product = (r0 & LONG_MASK) * (l3 & LONG_MASK)
                + (r1 & LONG_MASK) * (l2 & LONG_MASK)
                + (r2 & LONG_MASK) * (l1 & LONG_MASK)
                + (r3 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z3 = (int) product;
        if ((product >>> 32) != 0) {
            throwOverflowException();
        }

        setInts(result, z0, z1, z2, z3);
        setSignum(result, getSignum(left) * getSignum(right));

        throwIfOverflows(z0, z1, z2, z3);
    }

    public static void multiply(MutableDecimal left, MutableDecimal right, MutableDecimal result)
    {
        // the combinations below definitely result in overflow
        if ((right.v3 != 0 && (left.v3 != 0 || left.v2 != 0 || left.v1 != 0))
                || (right.v2 != 0 && (left.v3 != 0 || left.v2 != 0))
                || (right.v1 != 0 && left.v3 != 0)) {
            throwOverflowException();
        }

        long product;

        product = (right.v0 & LONG_MASK) * (left.v0 & LONG_MASK);
        result.v0 = (int) product;

        product = (right.v0 & LONG_MASK) * (left.v1 & LONG_MASK)
                + (right.v1 & LONG_MASK) * (left.v0 & LONG_MASK)
                + (product >> 32);
        result.v1 = (int) product;

        product = (right.v0 & LONG_MASK) * (left.v2 & LONG_MASK)
                + (right.v1 & LONG_MASK) * (left.v1 & LONG_MASK)
                + (right.v2 & LONG_MASK) * (left.v0 & LONG_MASK)
                + (product >> 32);
        result.v2 = (int) product;

        // v[3]
        product = (right.v0 & LONG_MASK) * (left.v3 & LONG_MASK)
                + (right.v1 & LONG_MASK) * (left.v2 & LONG_MASK)
                + (right.v2 & LONG_MASK) * (left.v1 & LONG_MASK)
                + (right.v3 & LONG_MASK) * (left.v0 & LONG_MASK)
                + (product >> 32);
        result.v3 = (int) product;
        if ((product >>> 32) != 0) {
            throwOverflowException();
        }

        result.signum = left.signum * right.signum;

        throwIfOverflows(result);
    }

    public static void negate(Slice decimal)
    {
        decimal.setByte(SIGNUM_BYTE_INDEX, -getSignum(decimal));
    }

    public static int getSignum(Slice decimal)
    {
        return unsafe.getByte(decimal.getBase(), decimal.getAddress() + SIGNUM_BYTE_INDEX);
    }

    public static void setSignum(Slice decimal, int signum)
    {
        unsafe.putByte(decimal.getBase(), decimal.getAddress() + SIGNUM_BYTE_INDEX, (byte) (signum & 0xFF));
    }

    private static void addUnsigned(Slice left, Slice right, Slice result)
    {
        long sum = (getInt(left, 0) & LONG_MASK) + (getInt(right, 0) & LONG_MASK);
        int v0 = (int) sum;
        sum = (getInt(left, 1) & LONG_MASK) + (getInt(right, 1) & LONG_MASK) + (sum >> 32);
        int v1 = (int) sum;
        sum = (getInt(left, 2) & LONG_MASK) + (getInt(right, 2) & LONG_MASK) + (sum >> 32);
        int v2 = (int) sum;
        sum = (getInt(left, 3) & LONG_MASK) + (getInt(right, 3) & LONG_MASK) + (sum >> 32);
        int v3 = (int) sum;

        if ((sum >> 32) != 0) {
            throwOverflowException();
        }

        setInts(result, v0, v1, v2, v3);
    }

    private static void addUnsigned(MutableDecimal left, MutableDecimal right, MutableDecimal result)
    {
        long sum = (left.v0 & LONG_MASK) + (right.v0 & LONG_MASK);
        result.v0 = (int) sum;
        sum = (left.v1 & LONG_MASK) + (right.v1 & LONG_MASK) + (sum >> 32);
        result.v1 = (int) sum;
        sum = (left.v2 & LONG_MASK) + (right.v2 & LONG_MASK) + (sum >> 32);
        result.v2 = (int) sum;
        sum = (left.v3 & LONG_MASK) + (right.v3 & LONG_MASK) + (sum >> 32);
        result.v3 = (int) sum;

        if ((sum >> 32) != 0) {
            throwOverflowException();
        }
    }

    private static int differenceUnsigned(Slice left, Slice right, Slice result)
    {
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        int cmp = compareUnsigned(l0, l1, l2, l3, r0, r1, r2, r3);
        if (cmp == 0) {
            result.setBytes(0, ZERO);
            return 0;
        }

        if (cmp > 0) {
            differenceLeftLarger(l0, l1, l2, l3, r0, r1, r2, r3, result);
        }
        else {
            differenceLeftLarger(r0, r1, r2, r3, l0, l1, l2, l3, result);
        }

        return cmp > 0 ? 1 : -1;
    }

    private static int differenceUnsigned(MutableDecimal left, MutableDecimal right, MutableDecimal result)
    {
        int cmp = compareUnsigned(left, right);
        if (cmp == 0) {
            result.zero();
            return 0;
        }

        if (cmp > 0) {
            differenceLeftLarger(left, right, result);
        }
        else {
            differenceLeftLarger(right, left, result);
        }

        return cmp > 0 ? 1 : -1;
    }

    private static void differenceLeftLarger(int l0, int l1, int l2, int l3, int r0, int r1, int r2, int r3, Slice result)
    {
        long sum = (l0 & LONG_MASK) - (r0 & LONG_MASK);
        int v0 = (int) sum;
        sum = (l1 & LONG_MASK) - (r1 & LONG_MASK) - ((int) -(sum >> 32));
        int v1 = (int) sum;
        sum = (l2 & LONG_MASK) - (r2 & LONG_MASK) - ((int) -(sum >> 32));
        int v2 = (int) sum;
        sum = (l3 & LONG_MASK) - (r3 & LONG_MASK) - ((int) -(sum >> 32));
        int v3 = (int) sum;

        if ((sum >> 32) != 0) {
            throwOverflowException();
        }

        setInts(result, v0, v1, v2, v3);
    }

    private static void differenceLeftLarger(MutableDecimal left, MutableDecimal right, MutableDecimal result)
    {
        long sum = (left.v0 & LONG_MASK) - (right.v0 & LONG_MASK);
        result.v0 = (int) sum;
        sum = (left.v1 & LONG_MASK) - (right.v1 & LONG_MASK) - ((int) -(sum >> 32));
        result.v1 = (int) sum;
        sum = (left.v2 & LONG_MASK) - (right.v2 & LONG_MASK) - ((int) -(sum >> 32));
        result.v2 = (int) sum;
        sum = (left.v3 & LONG_MASK) - (right.v3 & LONG_MASK) - ((int) -(sum >> 32));
        result.v3 = (int) sum;

        if ((sum >> 32) != 0) {
            throwOverflowException();
        }
    }

    private static int compareUnsigned(int l0, int l1, int l2, int l3, int r0, int r1, int r2, int r3)
    {
        if (l3 != r3) {
            return Integer.compareUnsigned(l3, r3);
        }
        if (l2 != r2) {
            return Integer.compareUnsigned(l2, r2);
        }
        if (l1 != r1) {
            return Integer.compareUnsigned(l1, r1);
        }
        if (l0 != r0) {
            return Integer.compareUnsigned(l0, r0);
        }
        return 0;
    }

    private static int compareUnsigned(MutableDecimal left, MutableDecimal right)
    {
        if (left.v3 != right.v3) {
            return Integer.compareUnsigned(left.v3, right.v3);
        }
        if (left.v2 != right.v2) {
            return Integer.compareUnsigned(left.v2, right.v2);
        }
        if (left.v1 != right.v1) {
            return Integer.compareUnsigned(left.v1, right.v1);
        }
        if (left.v0 != right.v0) {
            return Integer.compareUnsigned(left.v0, right.v0);
        }
        return 0;
    }

    private static void setInts(Slice decimal, int v0, int v1, int v2, int v3)
    {
        setInt(decimal, 0, v0);
        setInt(decimal, 1, v1);
        setInt(decimal, 2, v2);
        setInt(decimal, 3, v3);
    }

    private static int getInt(Slice decimal, int index)
    {
        return unsafe.getInt(decimal.getBase(), decimal.getAddress() + SIZE_OF_INT * index);
    }

    private static void setInt(Slice decimal, int index, int value)
    {
        unsafe.putInt(decimal.getBase(), decimal.getAddress() + SIZE_OF_INT * index, value);
    }

    private static void throwIfOverflows(Slice decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal)) {
            throwOverflowException();
        }
    }

    private static void throwIfOverflows(MutableDecimal decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal.v0, decimal.v1, decimal.v2, decimal.v3)) {
            throwOverflowException();
        }
    }

    private static void throwIfOverflows(int v0, int v1, int v2, int v3)
    {
        if (exceedsOrEqualTenToThirtyEight(v0, v1, v2, v3)) {
            throwOverflowException();
        }
    }

    private static boolean exceedsOrEqualTenToThirtyEight(Slice decimal)
    {
        // 10**38=
        // v[0]=0(0),v[1]=160047680(98a22400),v[2]=1518781562(5a86c47a),v[3]=1262177448(4b3b4ca8)

        // check most significant part first
        int v3 = getInt(decimal, 3);
        if (v3 >= 0 && v3 < 0x4b3b4ca8) {
            return false;
        }

        // check second most significant part
        if (v3 == 0x4b3b4ca8) {
            int v2 = getInt(decimal, 2);
            if (v2 >= 0 && v2 < 0x5a86c47a) {
                return false;
            }
        }
        else {
            return true;
        }

        int v2 = getInt(decimal, 2);
        int v1 = getInt(decimal, 1);
        return v2 != 0x5a86c47a || v1 < 0 || v1 >= 0x098a2240;
    }

    private static boolean exceedsOrEqualTenToThirtyEight(int v0, int v1, int v2, int v3)
    {
        // 10**38=
        // v[0]=0(0),v[1]=160047680(98a22400),v[2]=1518781562(5a86c47a),v[3]=1262177448(4b3b4ca8)

        // check most significant part first
        if (v3 >= 0 && v3 < 0x4b3b4ca8) {
            return false;
        }

        // check second most significant part
        if (v3 == 0x4b3b4ca8) {
            if (v2 >= 0 && v2 < 0x5a86c47a) {
                return false;
            }
        }
        else {
            return true;
        }

        return v2 != 0x5a86c47a || v1 < 0 || v1 >= 0x098a2240;
    }

    private static void throwOverflowException()
    {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "decimal overflow");
    }

    private static void reverse(byte[] array)
    {
        reverse(array, 0, array.length);
    }

    private static void reverse(byte[] array, final int startIndexInclusive, final int endIndexExclusive)
    {
        if (array == null) {
            return;
        }
        int i = startIndexInclusive < 0 ? 0 : startIndexInclusive;
        int j = Math.min(array.length, endIndexExclusive) - 1;
        byte tmp;
        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }

    private DecimalArithmetic() {}
}
