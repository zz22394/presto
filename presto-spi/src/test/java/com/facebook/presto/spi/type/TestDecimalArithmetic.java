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
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.DecimalArithmetic.ZERO;
import static com.facebook.presto.spi.type.DecimalArithmetic.add;
import static com.facebook.presto.spi.type.DecimalArithmetic.decimal;
import static com.facebook.presto.spi.type.DecimalArithmetic.decimalToBigInteger;
import static com.facebook.presto.spi.type.DecimalArithmetic.getSignum;
import static com.facebook.presto.spi.type.DecimalArithmetic.multiply;
import static com.facebook.presto.spi.type.DecimalArithmetic.negate;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDecimalArithmetic
{
    public static final Slice MAX_DECIMAL = decimal(new BigInteger("99999999999999999999999999999999999999"));
    public static final Slice MIN_DECIMAL = negateCopy(MAX_DECIMAL);

    @Test
    public void testUnscaledValueToDecimal()
    {
        assertEquals(decimal(0), decimal("0"));
        assertEquals(decimal(1), decimal("1"));
        assertEquals(decimal(-1), decimal("-1"));
        assertEquals(decimal(Long.MAX_VALUE), decimal(BigInteger.valueOf(Long.MAX_VALUE)));
        assertEquals(decimal(Long.MIN_VALUE), decimal(BigInteger.valueOf(Long.MIN_VALUE)));
    }

    @Test
    public void testDecimalToBigInteger()
    {
        assertConvertsBigInteger(new BigInteger("99999999999999999999999999999999999999"));
        assertConvertsBigInteger(new BigInteger("-99999999999999999999999999999999999999"));
        assertConvertsBigInteger(BigInteger.ZERO);
        assertConvertsBigInteger(BigInteger.ONE);
        assertConvertsBigInteger(BigInteger.ONE.negate());
    }

    @Test
    public void testAdd()
    {
        assertEquals(add(ZERO, ZERO), ZERO);
        assertEquals(add(ZERO, decimal(1)), decimal(1));
        assertEquals(add(ZERO, decimal(-1)), decimal(-1));
        assertEquals(add(decimal(1), ZERO), decimal(1));
        assertEquals(add(decimal(-1), ZERO), decimal(-1));
        assertEquals(add(decimal(-1), decimal(1)), ZERO);
        assertEquals(add(decimal(1), decimal(2)), decimal(3));
        assertEquals(add(decimal(Integer.MAX_VALUE), decimal(1)), decimal((long) Integer.MAX_VALUE + 1));
        assertEquals(add(decimal((long) Integer.MAX_VALUE * 2), decimal(1)), decimal((long) 2 * Integer.MAX_VALUE + 1));
        assertEquals(add(decimal(-Integer.MAX_VALUE), decimal(Long.MAX_VALUE)), decimal(Long.MAX_VALUE - Integer.MAX_VALUE));
        assertEquals(add(decimal(Integer.MAX_VALUE), decimal(-Long.MAX_VALUE)), decimal(-Long.MAX_VALUE + Integer.MAX_VALUE));
        assertEquals(add(decimal("99999999999999999999999999999999999999"), decimal("-99999999999999999999999999999999999999")), ZERO);
        assertEquals(add(decimal("99999999999999999999999999999999999999"), decimal("-999999999999999999999999")), decimal("99999999999999000000000000000000000000"));
        assertEquals(add(decimal("-999999999999999999999999"), decimal("99999999999999999999999999999999999999")), decimal("99999999999999000000000000000000000000"));
    }

    @Test
    public void testAddOverflows()
    {
        assertAddOverflows(MAX_DECIMAL, decimal(1));
        assertAddOverflows(MIN_DECIMAL, decimal(-1));
    }

    @Test
    public void testMultiply()
    {
        assertEquals(multiply(ZERO, MAX_DECIMAL), ZERO);
        assertEquals(multiply(ZERO, MIN_DECIMAL), ZERO);
        assertEquals(multiply(decimal(1), MAX_DECIMAL), MAX_DECIMAL);
        assertEquals(multiply(decimal(1), MIN_DECIMAL), MIN_DECIMAL);

        assertEquals(multiply(decimal(Integer.MAX_VALUE), decimal(Integer.MIN_VALUE)), decimal((long) Integer.MAX_VALUE * Integer.MIN_VALUE));
        assertEquals(multiply(decimal("99999999999999"), decimal("-1000000000000000000000000")), decimal("-99999999999999000000000000000000000000"));
    }

    @Test
    public void testMultiplyOverflows()
    {
        assertMultiplyOverflows(decimal("99999999999999"), decimal("-10000000000000000000000000"));
        assertMultiplyOverflows(MAX_DECIMAL, decimal("10"));
    }

    @Test
    public void testNegate()
    {
        assertEquals(negateCopy(negateCopy(MIN_DECIMAL)), MIN_DECIMAL);
        assertEquals(negateCopy(MIN_DECIMAL), MAX_DECIMAL);
        assertEquals(negateCopy(MIN_DECIMAL), MAX_DECIMAL);

        assertEquals(negateCopy(decimal(1)), decimal(-1));
        assertEquals(negateCopy(decimal(-1)), decimal(1));
    }

    @Test
    public void testIsNegative()
    {
        assertEquals(getSignum(MIN_DECIMAL), -1);
        assertEquals(getSignum(MAX_DECIMAL), 1);
        assertEquals(getSignum(ZERO), 0);
    }

    private static void assertAddOverflows(Slice left, Slice right)
    {
        try {
            add(left, right);
            fail();
        }
        catch (PrestoException ignored) {
        }
    }

    private static void assertMultiplyOverflows(Slice left, Slice right)
    {
        try {
            multiply(left, right);
            fail();
        }
        catch (PrestoException ignored) {
        }
    }

    private static void assertConvertsBigInteger(BigInteger value)
    {
        assertEquals(decimalToBigInteger(decimal(value)), value);
    }

    private static Slice negateCopy(Slice slice)
    {
        Slice copy = decimal(slice);
        negate(copy);
        return copy;
    }
}
