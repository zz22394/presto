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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestMathFunctions
        extends AbstractTestFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45, 0};
    private static final int[] intLefts = {9, 10, 11, -9, -10, -11, 0};
    private static final int[] intRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};
    private static final double GREATEST_DOUBLE_LESS_THAN_HALF = 0x1.fffffffffffffp-2;

    @Test
    public void testAbs()
    {
        assertFunction("abs(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("abs(TINYINT'-123')", TINYINT, (byte) 123);
        assertFunction("abs(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("abs(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("abs(SMALLINT'-123')", SMALLINT, (short) 123);
        assertFunction("abs(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("abs(123)", INTEGER, 123);
        assertFunction("abs(-123)", INTEGER, 123);
        assertFunction("abs(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("abs(BIGINT '123')", BIGINT, 123L);
        assertFunction("abs(BIGINT '-123')", BIGINT, 123L);
        assertFunction("abs(12300000000)", BIGINT, 12300000000L);
        assertFunction("abs(-12300000000)", BIGINT, 12300000000L);
        assertFunction("abs(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("abs(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("abs(DOUBLE '-123.0')", DOUBLE, 123.0);
        assertFunction("abs(DOUBLE '123.45')", DOUBLE, 123.45);
        assertFunction("abs(DOUBLE '-123.45')", DOUBLE, 123.45);
        assertFunction("abs(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("abs(FLOAT '-754.1985')", FLOAT, 754.1985f);
        assertInvalidFunction("abs(TINYINT'" + Byte.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(SMALLINT'" + Short.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(INTEGER'" + Integer.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(-9223372036854775807 - if(rand() < 10, 1, 1))", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("acos(" + doubleValue + ")", DOUBLE, Math.acos(doubleValue));
            assertFunction("acos(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.acos((float) doubleValue));
        }
        assertFunction("acos(NULL)", DOUBLE, null);
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("asin(" + doubleValue + ")", DOUBLE, Math.asin(doubleValue));
            assertFunction("asin(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.asin((float) doubleValue));
        }
        assertFunction("asin(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan(" + doubleValue + ")", DOUBLE, Math.atan(doubleValue));
            assertFunction("atan(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.atan((float) doubleValue));
        }
        assertFunction("atan(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan2(" + doubleValue + ", " + doubleValue + ")", DOUBLE, Math.atan2(doubleValue, doubleValue));
            assertFunction("atan2(FLOAT '" + (float) doubleValue + "', FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.atan2((float) doubleValue, (float) doubleValue));
        }
        assertFunction("atan2(NULL, NULL)", DOUBLE, null);
        assertFunction("atan2(1.0, NULL)", DOUBLE, null);
        assertFunction("atan2(NULL, 1.0)", DOUBLE, null);
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cbrt(" + doubleValue + ")", DOUBLE, Math.cbrt(doubleValue));
            assertFunction("cbrt(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.cbrt((float) doubleValue));
        }
        assertFunction("cbrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testCeil()
    {
        assertFunction("ceil(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("ceil(TINYINT'-123')", TINYINT, (byte) -123);
        assertFunction("ceil(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("ceil(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("ceil(SMALLINT'-123')", SMALLINT, (short) -123);
        assertFunction("ceil(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("ceil(123)", INTEGER, 123);
        assertFunction("ceil(-123)", INTEGER, -123);
        assertFunction("ceil(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("ceil(BIGINT '123')", BIGINT, 123L);
        assertFunction("ceil(BIGINT '-123')", BIGINT, -123L);
        assertFunction("ceil(12300000000)", BIGINT, 12300000000L);
        assertFunction("ceil(-12300000000)", BIGINT, -12300000000L);
        assertFunction("ceil(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("ceil(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("ceil(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("ceil(DOUBLE '123.45')", DOUBLE, 124.0);
        assertFunction("ceil(DOUBLE '-123.45')", DOUBLE, -123.0);
        assertFunction("ceil(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("ceil(FLOAT '123.0')", FLOAT, 123.0f);
        assertFunction("ceil(FLOAT '-123.0')", FLOAT, -123.0f);
        assertFunction("ceil(FLOAT '123.45')", FLOAT, 124.0f);
        assertFunction("ceil(FLOAT '-123.45')", FLOAT, -123.0f);

        assertFunction("ceiling(12300000000)", BIGINT, 12300000000L);
        assertFunction("ceiling(-12300000000)", BIGINT, -12300000000L);
        assertFunction("ceiling(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("ceiling(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("ceiling(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("ceiling(DOUBLE '123.45')", DOUBLE, 124.0);
        assertFunction("ceiling(DOUBLE '-123.45')", DOUBLE, -123.0);
        assertFunction("ceiling(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("ceiling(FLOAT '123.0')", FLOAT, 123.0f);
        assertFunction("ceiling(FLOAT '-123.0')", FLOAT, -123.0f);
        assertFunction("ceiling(FLOAT '123.45')", FLOAT, 124.0f);
        assertFunction("ceiling(FLOAT '-123.45')", FLOAT, -123.0f);
        assertFunction("ceiling(CAST(NULL AS FLOAT))", FLOAT, null);
    }

    @Test
    public void testTruncate()
    {
        final String maxDouble = Double.toString(Double.MAX_VALUE);
        final String minDouble = Double.toString(-Double.MAX_VALUE);
        assertFunction("truncate(DOUBLE '17.18')", DOUBLE, 17.0);
        assertFunction("truncate(DOUBLE '-17.18')", DOUBLE, -17.0);
        assertFunction("truncate(DOUBLE '17.88')", DOUBLE, 17.0);
        assertFunction("truncate(DOUBLE '-17.88')", DOUBLE, -17.0);
        assertFunction("truncate(FLOAT '17.18')", FLOAT, 17.0f);
        assertFunction("truncate(FLOAT '-17.18')", FLOAT, -17.0f);
        assertFunction("truncate(FLOAT '17.88')", FLOAT, 17.0f);
        assertFunction("truncate(FLOAT '-17.88')", FLOAT, -17.0f);
        assertFunction("truncate(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("truncate(DOUBLE '" + maxDouble + "')", DOUBLE, Double.MAX_VALUE);
        assertFunction("truncate(DOUBLE '" + minDouble + "')", DOUBLE, -Double.MAX_VALUE);
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cos(" + doubleValue + ")", DOUBLE, Math.cos(doubleValue));
            assertFunction("cos(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.cos((float) doubleValue));
        }
        assertFunction("cos(NULL)", DOUBLE, null);
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cosh(" + doubleValue + ")", DOUBLE, Math.cosh(doubleValue));
            assertFunction("cosh(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.cosh((float) doubleValue));
        }
        assertFunction("cosh(NULL)", DOUBLE, null);
    }

    @Test
    public void testDegrees()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("degrees(%s)", doubleValue), DOUBLE, Math.toDegrees(doubleValue));
            assertFunction(String.format("degrees(FLOAT '%s')", (float) doubleValue), DOUBLE, Math.toDegrees((float) doubleValue));
        }
        assertFunction("degrees(NULL)", DOUBLE, null);
    }

    @Test
    public void testE()
    {
        assertFunction("e()", DOUBLE, Math.E);
    }

    @Test
    public void testExp()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("exp(" + doubleValue + ")", DOUBLE, Math.exp(doubleValue));
            assertFunction("exp(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.exp((float) doubleValue));
        }
        assertFunction("exp(NULL)", DOUBLE, null);
    }

    @Test
    public void testFloor()
    {
        assertFunction("floor(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("floor(TINYINT'-123')", TINYINT, (byte) -123);
        assertFunction("floor(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("floor(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("floor(SMALLINT'-123')", SMALLINT, (short) -123);
        assertFunction("floor(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("floor(123)", INTEGER, 123);
        assertFunction("floor(-123)", INTEGER, -123);
        assertFunction("floor(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("floor(BIGINT '123')", BIGINT, 123L);
        assertFunction("floor(BIGINT '-123')", BIGINT, -123L);
        assertFunction("floor(12300000000)", BIGINT, 12300000000L);
        assertFunction("floor(-12300000000)", BIGINT, -12300000000L);
        assertFunction("floor(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("floor(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("floor(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("floor(DOUBLE '123.45')", DOUBLE, 123.0);
        assertFunction("floor(DOUBLE '-123.45')", DOUBLE, -124.0);
        assertFunction("floor(FLOAT '123.0')", FLOAT, 123.0f);
        assertFunction("floor(FLOAT '-123.0')", FLOAT, -123.0f);
        assertFunction("floor(FLOAT '123.45')", FLOAT, 123.0f);
        assertFunction("floor(FLOAT '-123.45')", FLOAT, -124.0f);
        assertFunction("floor(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("floor(CAST(NULL as FLOAT))", FLOAT, null);
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("ln(" + doubleValue + ")", DOUBLE, Math.log(doubleValue));
        }
        assertFunction("ln(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log2(" + doubleValue + ")", DOUBLE, Math.log(doubleValue) / Math.log(2));
        }
        assertFunction("log2(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log10(" + doubleValue + ")", DOUBLE, Math.log10(doubleValue));
        }
        assertFunction("log10(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertFunction("log(" + doubleValue + ", " + base + ")", DOUBLE, Math.log(doubleValue) / Math.log(base));
                assertFunction("log(FLOAT '" + (float) doubleValue + "', FLOAT'" + (float) base + "')", DOUBLE, Math.log((float) doubleValue) / Math.log((float) base));
            }
        }
        assertFunction("log(NULL, NULL)", DOUBLE, null);
        assertFunction("log(5.0, NULL)", DOUBLE, null);
        assertFunction("log(NULL, 5.0)", DOUBLE, null);
    }

    @Test
    public void testMod()
    {
        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("mod(" + left + ", " + right + ")", INTEGER, (left % right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("mod( BIGINT '" + left + "' , BIGINT '" + right + "')", BIGINT, (long) (left % right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("mod(" + left * 10000000000L + ", " + right * 10000000000L + ")", BIGINT, (left * 10000000000L) % (right * 10000000000L));
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", FLOAT '" + (float) right + "')", FLOAT, left % (float) right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("mod(DOUBLE '" + left + "', " + right + ")", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("mod(FLOAT '" + (float) left + "', " + right + ")", FLOAT, (float) left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(DOUBLE '" + left + "', DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(FLOAT '" + (float) left + "', FLOAT '" + (float) right + "')", FLOAT, (float) left % (float) right);
            }
        }

        assertFunction("mod(DOUBLE '5.0', NULL)", DOUBLE, null);
        assertFunction("mod(NULL, DOUBLE '5.0')", DOUBLE, null);
    }

    @Test
    public void testPi()
    {
        assertFunction("pi()", DOUBLE, Math.PI);
    }

    @Test
    public void testNaN()
    {
        assertFunction("nan()", DOUBLE, Double.NaN);
        assertFunction("CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE)", DOUBLE, Double.NaN);
    }

    @Test
    public void testInfinity()
    {
        assertFunction("infinity()", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("-rand() / 0.0", DOUBLE, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testIsInfinite()
    {
        assertFunction("is_infinite(DOUBLE '1.0' / DOUBLE '0.0')", BOOLEAN, true);
        assertFunction("is_infinite(DOUBLE '0.0' / DOUBLE '0.0')", BOOLEAN, false);
        assertFunction("is_infinite(DOUBLE '1.0' / DOUBLE '1.0')", BOOLEAN, false);
        assertFunction("is_infinite(FLOAT '1.0' / FLOAT '0.0')", BOOLEAN, true);
        assertFunction("is_infinite(FLOAT '0.0' / FLOAT '0.0')", BOOLEAN, false);
        assertFunction("is_infinite(FLOAT '1.0' / FLOAT '1.0')", BOOLEAN, false);
        assertFunction("is_infinite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsFinite()
    {
        assertFunction("is_finite(100000)", BOOLEAN, true);
        assertFunction("is_finite(rand() / 0.0)", BOOLEAN, false);
        assertFunction("is_finite(FLOAT '754.2008')", BOOLEAN, true);
        assertFunction("is_finite(rand() / FLOAT '0.0')", BOOLEAN, false);
        assertFunction("is_finite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsNaN()
    {
        assertFunction("is_nan(CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", BOOLEAN, true);
        assertFunction("is_nan(CAST(0.0 as DOUBLE) / CAST(1.0 as DOUBLE))", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
        assertFunction("is_nan(FLOAT '0.0' / FLOAT '0.0')", BOOLEAN, true);
        assertFunction("is_nan(FLOAT '0.0' / 1.0)", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
        assertFunction("is_nan(NULL)", BOOLEAN, null);
    }

    @Test
    public void testPower()
    {
        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("power( BIGINT '" + left + "' , BIGINT '" + right + "')", DOUBLE, Math.pow(left, right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left * 10000000000L + ", " + right + ")", DOUBLE, Math.pow(left * 10000000000L, right));
            }
        }

        for (long left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(" + left + ", FLOAT '" + (float) right + "')", DOUBLE, Math.pow(left, (float) right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(FLOAT '" + (float) left + "', " + right + ")", DOUBLE, Math.pow((float) left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(FLOAT '" + left + "', FLOAT '" + right + "')", DOUBLE, Math.pow((float) left, (float) right));
            }
        }

        assertFunction("power(NULL, NULL)", DOUBLE, null);
        assertFunction("power(5.0, NULL)", DOUBLE, null);
        assertFunction("power(NULL, 5.0)", DOUBLE, null);

        // test alias
        assertFunction("pow(5.0, 2.0)", DOUBLE, 25.0);
    }

    @Test
    public void testRadians()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("radians(%s)", doubleValue), DOUBLE, Math.toRadians(doubleValue));
            assertFunction(String.format("radians(FLOAT '%s')", (float) doubleValue), DOUBLE, Math.toRadians((float) doubleValue));
        }
        assertFunction("radians(NULL)", DOUBLE, null);
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        functionAssertions.tryEvaluateWithAll("rand()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("rand(1000)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(2000)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(3000000000)", BIGINT, TEST_SESSION);

        assertInvalidFunction("rand(-1)", "bound must be positive");
        assertInvalidFunction("rand(-3000000000)", "bound must be positive");
    }

    @Test
    public void testRound()
    {
        assertFunction("round(TINYINT '3')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '-3')", TINYINT, (byte) -3);
        assertFunction("round(CAST(NULL as TINYINT))", TINYINT, null);
        assertFunction("round(SMALLINT '3')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '-3')", SMALLINT, (short) -3);
        assertFunction("round(CAST(NULL as SMALLINT))", SMALLINT, null);
        assertFunction("round(3)", INTEGER, 3);
        assertFunction("round(-3)", INTEGER, -3);
        assertFunction("round(CAST(NULL as INTEGER))", INTEGER, null);
        assertFunction("round(BIGINT '3')", BIGINT, 3L);
        assertFunction("round(BIGINT '-3')", BIGINT, -3L);
        assertFunction("round(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round( 3000000000)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000)", BIGINT, -3000000000L);
        assertFunction("round(DOUBLE '3.0')", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0')", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499')", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.499')", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.5')", DOUBLE, 4.0);
        assertFunction("round(DOUBLE '-3.5')", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.5001')", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.99')", DOUBLE, -4.0);
        assertFunction("round(FLOAT '3.0')", FLOAT, 3.0f);
        assertFunction("round(FLOAT '-3.0')", FLOAT, -3.0f);
        assertFunction("round(FLOAT '3.499')", FLOAT, 3.0f);
        assertFunction("round(FLOAT '-3.499')", FLOAT, -3.0f);
        assertFunction("round(FLOAT '3.5')", FLOAT, 4.0f);
        assertFunction("round(FLOAT '-3.5')", FLOAT, -4.0f);
        assertFunction("round(FLOAT '-3.5001')", FLOAT, -4.0f);
        assertFunction("round(FLOAT '-3.99')", FLOAT,  -4.0f);
        assertFunction("round(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("round(DOUBLE '" + GREATEST_DOUBLE_LESS_THAN_HALF + "')", DOUBLE, 0.0);
        assertFunction("round(DOUBLE '-" + 0x1p-1 + "')", DOUBLE, -1.0); // -0.5
        assertFunction("round(DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "')", DOUBLE, -0.0);

        assertFunction("round(TINYINT '3', TINYINT '0')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '3', 0)", TINYINT, (byte) 3);
        assertFunction("round(SMALLINT '3', SMALLINT '0')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '3', 0)", SMALLINT, (short) 3);
        assertFunction("round(3, 0)", INTEGER, 3);
        assertFunction("round(-3, 0)", INTEGER, -3);
        assertFunction("round(-3, BIGINT '0')", INTEGER, -3);
        assertFunction("round(BIGINT '3', 0)", BIGINT, 3L);
        assertFunction("round( 3000000000, 0)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000, 0)", BIGINT, -3000000000L);
        assertFunction("round(DOUBLE '3.0', 0)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0', 0)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499', 0)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.499', 0)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.5', 0)", DOUBLE, 4.0);
        assertFunction("round(DOUBLE '-3.5', 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.5001', 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.99', 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '" + GREATEST_DOUBLE_LESS_THAN_HALF + "', 0)", DOUBLE, 0.0);
        assertFunction("round(DOUBLE '-" + 0x1p-1 + "')", DOUBLE, -1.0); // -0.5
        assertFunction("round(DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "', 0)", DOUBLE, -0.0);
        assertFunction("round(DOUBLE '0.3')", DOUBLE, 0.0);
        assertFunction("round(DOUBLE '-0.3')", DOUBLE, -0.0);

        assertFunction("round(TINYINT '3', TINYINT '1')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '3', 1)", TINYINT, (byte) 3);
        assertFunction("round(SMALLINT '3', SMALLINT '1')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '3', 1)", SMALLINT, (short) 3);
        assertFunction("round(FLOAT '3.0', 0)", FLOAT, 3.0f);
        assertFunction("round(FLOAT '-3.0', 0)", FLOAT, -3.0f);
        assertFunction("round(FLOAT '3.499', 0)", FLOAT, 3.0f);
        assertFunction("round(FLOAT '-3.499', 0)", FLOAT, -3.0f);
        assertFunction("round(FLOAT '3.5', 0)", FLOAT, 4.0f);
        assertFunction("round(FLOAT '-3.5', 0)", FLOAT, -4.0f);
        assertFunction("round(FLOAT '-3.5001', 0)", FLOAT, -4.0f);
        assertFunction("round(FLOAT '-3.99', 0)", FLOAT, -4.0f);
        assertFunction("round(3, 1)", INTEGER, 3);
        assertFunction("round(-3, 1)", INTEGER, -3);
        assertFunction("round(-3, BIGINT '1')", INTEGER, -3);
        assertFunction("round(-3, CAST(NULL as BIGINT))", INTEGER, null);
        assertFunction("round(BIGINT '3', 1)", BIGINT, 3L);
        assertFunction("round( 3000000000, 1)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000, 1)", BIGINT, -3000000000L);
        assertFunction("round(CAST(NULL as BIGINT), CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round(CAST(NULL as BIGINT), 1)", BIGINT, null);
        assertFunction("round(DOUBLE '3.0', 1)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0', 1)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499', 1)", DOUBLE, 3.5);
        assertFunction("round(DOUBLE '-3.499', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '3.5', 1)", DOUBLE, 3.5);
        assertFunction("round(DOUBLE '-3.5', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '-3.5001', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '-3.99', 1)", DOUBLE, -4.0);
        assertFunction("round(FLOAT '3.0', 1)", FLOAT, 3.0f);
        assertFunction("round(FLOAT '-3.0', 1)", FLOAT, -3.0f);
        assertFunction("round(FLOAT '3.499', 1)", FLOAT, 3.5f);
        assertFunction("round(FLOAT '-3.499', 1)", FLOAT, -3.5f);
        assertFunction("round(FLOAT '3.5', 1)", FLOAT, 3.5f);
        assertFunction("round(FLOAT '-3.5', 1)", FLOAT, -3.5f);
        assertFunction("round(FLOAT '-3.5001', 1)", FLOAT, -3.5f);
        assertFunction("round(FLOAT '-3.99', 1)", FLOAT, -4.0f);

        assertFunction("round(CAST(NULL as DOUBLE), CAST(NULL as BIGINT))", DOUBLE, null);
        assertFunction("round(DOUBLE '-3.0', CAST(NULL as BIGINT))", DOUBLE, null);
        assertFunction("round(CAST(NULL as DOUBLE), 1)", DOUBLE, null);

        assertFunction("round(nan(), 2)", DOUBLE, Double.NaN);
        assertFunction("round(DOUBLE '1.0' / 0, 2)", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("round(DOUBLE '-1.0' / 0, 2)", DOUBLE, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testSign()
    {
        //retains type for NULL values
        assertFunction("sign(CAST(NULL as TINYINT))", TINYINT, null);
        assertFunction("sign(CAST(NULL as SMALLINT))", SMALLINT, null);
        assertFunction("sign(CAST(NULL as INTEGER))", INTEGER, null);
        assertFunction("sign(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("sign(CAST(NULL as DOUBLE))", DOUBLE, null);

        //tinyint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(TINYINT '" + intValue + "')", TINYINT, signum.byteValue());
        }

        //smallint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(SMALLINT '" + intValue + "')", SMALLINT, signum.shortValue());
        }

        //integer
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(INTEGER '" + intValue + "')", INTEGER, signum.intValue());
        }

        //bigint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(BIGINT '" + intValue + "')", BIGINT, signum.longValue());
        }

        //double and float
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sign(DOUBLE '" + doubleValue + "')", DOUBLE, Math.signum(doubleValue));
            assertFunction("sign(FLOAT '" + (float) doubleValue + "')", FLOAT, Math.signum(((float) doubleValue)));
        }

        //returns NaN for NaN input
        assertFunction("sign(DOUBLE 'NaN')", DOUBLE, Double.NaN);

        //returns proper sign for +/-Infinity input
        assertFunction("sign(DOUBLE '+Infinity')", DOUBLE, 1.0);
        assertFunction("sign(DOUBLE '-Infinity')", DOUBLE, -1.0);
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sin(" + doubleValue + ")", DOUBLE, Math.sin(doubleValue));
            assertFunction("sin(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.sin((float) doubleValue));
        }
        assertFunction("sin(NULL)", DOUBLE, null);
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sqrt(" + doubleValue + ")", DOUBLE, Math.sqrt(doubleValue));
            assertFunction("sqrt(FLOAT '" + doubleValue + "')", DOUBLE, Math.sqrt((float) doubleValue));
        }
        assertFunction("sqrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tan(" + doubleValue + ")", DOUBLE, Math.tan(doubleValue));
            assertFunction("tan(FLOAT '" + (float) doubleValue + "')", DOUBLE, Math.tan((float) doubleValue));
        }
        assertFunction("tan(NULL)", DOUBLE, null);
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tanh(" + doubleValue + ")", DOUBLE, Math.tanh(doubleValue));
            assertFunction("tanh(FLOAT '" + doubleValue + "')", DOUBLE, Math.tanh((float) doubleValue));
        }
        assertFunction("tanh(NULL)", DOUBLE, null);
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        // tinyint
        assertFunction("greatest(TINYINT'1', TINYINT'2')", TINYINT, (byte) 2);
        assertFunction("greatest(TINYINT'-1', TINYINT'-2')", TINYINT, (byte) -1);
        assertFunction("greatest(TINYINT'5', TINYINT'4', TINYINT'3', TINYINT'2', TINYINT'1', TINYINT'2', TINYINT'3', TINYINT'4', TINYINT'1', TINYINT'5')", TINYINT, (byte) 5);
        assertFunction("greatest(TINYINT'-1')", TINYINT, (byte) -1);
        assertFunction("greatest(TINYINT'5', TINYINT'4', CAST(NULL AS TINYINT), TINYINT'3')", TINYINT, null);

        // smallint
        assertFunction("greatest(SMALLINT'1', SMALLINT'2')", SMALLINT, (short) 2);
        assertFunction("greatest(SMALLINT'-1', SMALLINT'-2')", SMALLINT, (short) -1);
        assertFunction("greatest(SMALLINT'5', SMALLINT'4', SMALLINT'3', SMALLINT'2', SMALLINT'1', SMALLINT'2', SMALLINT'3', SMALLINT'4', SMALLINT'1', SMALLINT'5')", SMALLINT, (short) 5);
        assertFunction("greatest(SMALLINT'-1')", SMALLINT, (short) -1);
        assertFunction("greatest(SMALLINT'5', SMALLINT'4', CAST(NULL AS SMALLINT), SMALLINT'3')", SMALLINT, null);

        // integer
        assertFunction("greatest(1, 2)", INTEGER, 2);
        assertFunction("greatest(-1, -2)", INTEGER, -1);
        assertFunction("greatest(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", INTEGER, 5);
        assertFunction("greatest(-1)", INTEGER, -1);
        assertFunction("greatest(5, 4, CAST(NULL AS INTEGER), 3)", INTEGER, null);

        // bigint
        assertFunction("greatest(10000000000, 20000000000)", BIGINT, 20000000000L);
        assertFunction("greatest(-10000000000, -20000000000)", BIGINT, -10000000000L);
        assertFunction("greatest(5000000000, 4, 3, 2, 1000000000, 2, 3, 4, 1, 5000000000)", BIGINT, 5000000000L);
        assertFunction("greatest(-10000000000)", BIGINT, -10000000000L);
        assertFunction("greatest(5000000000, 4000000000, CAST(NULL as BIGINT), 3000000000)", BIGINT, null);

        // double
        assertFunction("greatest(CAST(CAST(1.5 as DOUBLE) as DOUBLE), CAST(2.3 as DOUBLE))", DOUBLE, 2.3);
        assertFunction("greatest(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE))", DOUBLE, -1.5);
        assertFunction("greatest(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE), -5/3)", DOUBLE, -1.0);
        assertFunction("greatest(CAST(1.5 as DOUBLE), -CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE), CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE))", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("greatest(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // float
        assertFunction("greatest(FLOAT '1.5', DOUBLE '2.3')", DOUBLE, 2.3);
        assertFunction("greatest(FLOAT '-1.5', DOUBLE '-2.3')", DOUBLE, (double) -1.5f);
        assertFunction("greatest(DOUBLE '-1.5', FLOAT '-2.3', -5/3)", DOUBLE, -1.0);
        assertFunction("greatest(FLOAT '1.5', FLOAT '-1.0' / DOUBLE '0.0', 1.0 / FLOAT '0.0')", DOUBLE, (double) (1.0f / 0.0f));
        assertFunction("greatest(5, FLOAT '4', CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // decimal
        assertDecimalFunction("greatest(1.0, 2.0)", decimal("2.0"));
        assertDecimalFunction("greatest(1.0, -2.0)", decimal("1.0"));
        assertDecimalFunction("greatest(1.0, 1.1, 1.2, 1.3)", decimal("1.3"));

        // mixed
        assertFunction("greatest(1, 20000000000)", BIGINT, 20000000000L);
        assertFunction("greatest(1, BIGINT '2')", BIGINT, 2L);
        assertFunction("greatest(DOUBLE '1.0', INTEGER '2')", DOUBLE, 2.0);
        assertFunction("greatest(1, CAST(2.0 as DOUBLE))", DOUBLE, 2.0);
        assertFunction("greatest(CAST(1.0 as DOUBLE), 2)", DOUBLE, 2.0);
        assertFunction("greatest(CAST(5.0 as DOUBLE), 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("greatest(CAST(5.0 as DOUBLE), 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("greatest(1.0, CAST(2.0 as DOUBLE))", DOUBLE, 2.0);
        assertDecimalFunction("greatest(5, 4, 3.0, 2)", decimal("0000000005.0"));

        // invalid
        assertInvalidFunction("greatest(CAST(1.5 as DOUBLE), CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", "Invalid argument to greatest(): NaN");
    }

    @Test
    public void testLeast()
            throws Exception
    {
        // integer
        assertFunction("least(TINYINT'1', TINYINT'2')", TINYINT, (byte) 1);
        assertFunction("least(TINYINT'-1', TINYINT'-2')", TINYINT, (byte) -2);
        assertFunction("least(TINYINT'5', TINYINT'4', TINYINT'3', TINYINT'2', TINYINT'1', TINYINT'2', TINYINT'3', TINYINT'4', TINYINT'1', TINYINT'5')", TINYINT, (byte) 1);
        assertFunction("least(TINYINT'-1')", TINYINT, (byte) -1);
        assertFunction("least(TINYINT'5', TINYINT'4', CAST(NULL AS TINYINT), TINYINT'3')", TINYINT, null);

        // integer
        assertFunction("least(SMALLINT'1', SMALLINT'2')", SMALLINT, (short) 1);
        assertFunction("least(SMALLINT'-1', SMALLINT'-2')", SMALLINT, (short) -2);
        assertFunction("least(SMALLINT'5', SMALLINT'4', SMALLINT'3', SMALLINT'2', SMALLINT'1', SMALLINT'2', SMALLINT'3', SMALLINT'4', SMALLINT'1', SMALLINT'5')", SMALLINT, (short) 1);
        assertFunction("least(SMALLINT'-1')", SMALLINT, (short) -1);
        assertFunction("least(SMALLINT'5', SMALLINT'4', CAST(NULL AS SMALLINT), SMALLINT'3')", SMALLINT, null);

        // integer
        assertFunction("least(1, 2)", INTEGER, 1);
        assertFunction("least(-1, -2)", INTEGER, -2);
        assertFunction("least(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", INTEGER, 1);
        assertFunction("least(-1)", INTEGER, -1);
        assertFunction("least(5, 4, CAST(NULL AS INTEGER), 3)", INTEGER, null);

        // bigint
        assertFunction("least(10000000000, 20000000000)", BIGINT, 10000000000L);
        assertFunction("least(-10000000000, -20000000000)", BIGINT, -20000000000L);
        assertFunction("least(50000000000, 40000000000, 30000000000, 20000000000, 50000000000)", BIGINT, 20000000000L);
        assertFunction("least(-10000000000)", BIGINT, -10000000000L);
        assertFunction("least(500000000, 400000000, CAST(NULL as BIGINT), 300000000)", BIGINT, null);

        // double
        assertFunction("least(CAST(1.5 as DOUBLE), CAST(2.3 as DOUBLE))", DOUBLE, 1.5);
        assertFunction("least(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE))", DOUBLE, -2.3);
        assertFunction("least(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE), -5/3)", DOUBLE, -2.3);
        assertFunction("least(CAST(1.5 as DOUBLE), -CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE), CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE))", DOUBLE, Double.NEGATIVE_INFINITY);
        assertFunction("least(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // float
        assertFunction("least(FLOAT '1.5', DOUBLE '2.3')", DOUBLE, (double) 1.5f);
        assertFunction("least(FLOAT '-1.5', DOUBLE '-2.3')", DOUBLE, -2.3);
        assertFunction("least(DOUBLE '-2.3', FLOAT '-0.4', -5/3)", DOUBLE, -2.3);
        assertFunction("least(DOUBLE '1.5', FLOAT '-1.0' / 0.0, DOUBLE '1.0' / 0.0)", DOUBLE, (double) (-1.0f / 0.0f));
        assertFunction("least(FLOAT '5', 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // decimal
        assertDecimalFunction("least(1.0, 2.0)", decimal("1.0"));
        assertDecimalFunction("least(1.0, -2.0)", decimal("-2.0"));
        assertDecimalFunction("least(1.0, 1.1, 1.2, 1.3)", decimal("1.0"));

        // mixed
        assertFunction("least(1, 20000000000)", BIGINT, 1L);
        assertFunction("least(1, BIGINT '2')", BIGINT, 1L);
        assertFunction("least(CAST(1.0 as DOUBLE), 2)", DOUBLE, 1.0);
        assertFunction("least(1, CAST(2.0 as DOUBLE))", DOUBLE, 1.0);
        assertFunction("least(CAST(1.0 as DOUBLE), 2)", DOUBLE, 1.0);
        assertFunction("least(CAST(5.0 as DOUBLE), 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("least(CAST(5.0 as DOUBLE), 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("least(1.0, CAST(2.0 as DOUBLE))", DOUBLE, 1.0);
        assertDecimalFunction("least(5, 4, 3.0, 2)", decimal("0000000002.0"));

        // invalid
        assertInvalidFunction("least(CAST(1.5 as DOUBLE), CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", "Invalid argument to least(): NaN");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "\\QInvalid argument to greatest(): NaN\\E")
    public void testGreatestWithNaN()
            throws Exception
    {
        functionAssertions.tryEvaluate("greatest(DOUBLE '1.5', DOUBLE '0.0' / DOUBLE '0.0')", DOUBLE);
        functionAssertions.tryEvaluate("greatest(DOUBLE '1.5', FLOAT '0.0' / FLOAT '0.0')", DOUBLE);
    }

    @Test
    public void testToBase()
            throws Exception
    {
        assertFunction("to_base(2147483648, 16)", VARCHAR, "80000000");
        assertFunction("to_base(255, 2)", VARCHAR, "11111111");
        assertFunction("to_base(-2147483647, 16)", VARCHAR, "-7fffffff");
        assertFunction("to_base(NULL, 16)", VARCHAR, null);
        assertFunction("to_base(-2147483647, NULL)", VARCHAR, null);
        assertFunction("to_base(NULL, NULL)", VARCHAR, null);
        assertInvalidFunction("to_base(255, 1)", "Radix must be between 2 and 36");
        assertInvalidFunction("to_base(255, 1)", "Radix must be between 2 and 36");
    }

    @Test
    public void testFromBase()
            throws Exception
    {
        assertFunction("from_base('80000000', 16)", BIGINT, 2147483648L);
        assertFunction("from_base('11111111', 2)", BIGINT, 255L);
        assertFunction("from_base('-7fffffff', 16)", BIGINT, -2147483647L);
        assertFunction("from_base('9223372036854775807', 10)", BIGINT, 9223372036854775807L);
        assertFunction("from_base('-9223372036854775808', 10)", BIGINT, -9223372036854775808L);
        assertFunction("from_base(NULL, 10)", BIGINT, null);
        assertFunction("from_base('-9223372036854775808', NULL)", BIGINT, null);
        assertFunction("from_base(NULL, NULL)", BIGINT, null);
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
    }

    @Test
    public void testWidthBucket()
            throws Exception
    {
        assertFunction("width_bucket(3.14, 0, 4, 3)", BIGINT, 3L);
        assertFunction("width_bucket(2, 0, 4, 3)", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), 0, 4, 3)", BIGINT, 4L);
        assertFunction("width_bucket(-1, 0, 3.2, 4)", BIGINT, 0L);

        // bound1 > bound2 is not symmetric with bound2 > bound1
        assertFunction("width_bucket(3.14, 4, 0, 3)", BIGINT, 1L);
        assertFunction("width_bucket(2, 4, 0, 3)", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), 4, 0, 3)", BIGINT, 0L);
        assertFunction("width_bucket(-1, 3.2, 0, 4)", BIGINT, 5L);

        // failure modes
        assertInvalidFunction("width_bucket(3.14, 0, 4, 0)", "bucketCount must be greater than 0");
        assertInvalidFunction("width_bucket(3.14, 0, 4, -1)", "bucketCount must be greater than 0");
        assertInvalidFunction("width_bucket(nan(), 0, 4, 3)", "operand must not be NaN");
        assertInvalidFunction("width_bucket(3.14, -1, -1, 3)", "bounds cannot equal each other");
        assertInvalidFunction("width_bucket(3.14, nan(), -1, 3)", "first bound must be finite");
        assertInvalidFunction("width_bucket(3.14, -1, nan(), 3)", "second bound must be finite");
        assertInvalidFunction("width_bucket(3.14, infinity(), -1, 3)", "first bound must be finite");
        assertInvalidFunction("width_bucket(3.14, -1, infinity(), 3)", "second bound must be finite");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Bucket for value Infinity is out of range")
    public void testWidthBucketOverflowAscending()
            throws Exception
    {
        functionAssertions.tryEvaluate("width_bucket(infinity(), 0, 4, " + Long.MAX_VALUE + ")", DOUBLE);
        functionAssertions.tryEvaluate("width_bucket(cast(infinity() as float), 0, 4, " + Long.MAX_VALUE + ")", DOUBLE);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Bucket for value Infinity is out of range")
    public void testWidthBucketOverflowDescending()
            throws Exception
    {
        functionAssertions.tryEvaluate("width_bucket(infinity(), 4, 0, " + Long.MAX_VALUE + ")", DOUBLE);
        functionAssertions.tryEvaluate("width_bucket(cast(infinity() as float), 4, 0, " + Long.MAX_VALUE + ")", DOUBLE);
    }

    @Test
    public void testWidthBucketArray()
            throws Exception
    {
        assertFunction("width_bucket(3.14, array[0.0, 2.0, 4.0])", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), array[0.0, 2.0, 4.0])", BIGINT, 3L);
        assertFunction("width_bucket(-1, array[0.0, 1.2, 3.3, 4.5])", BIGINT, 0L);

        // edge case of only a single bin
        assertFunction("width_bucket(3.145, array[0.0])", BIGINT, 1L);
        assertFunction("width_bucket(-3.145, array[0.0])", BIGINT, 0L);

        // failure modes
        assertInvalidFunction("width_bucket(3.14, array[])", "Bins cannot be an empty array");
        assertInvalidFunction("width_bucket(nan(), array[1.0, 2.0, 3.0])", "Operand cannot be NaN");
        assertInvalidFunction("width_bucket(3.14, array[0.0, infinity()])", "Bin value must be finite, got Infinity");

        // fail if we aren't sorted
        assertInvalidFunction("width_bucket(3.145, array[1.0, 0.0])", "Bin values are not sorted in ascending order");
        assertInvalidFunction("width_bucket(3.145, array[1.0, 0.0, -1.0])", "Bin values are not sorted in ascending order");
        assertInvalidFunction("width_bucket(3.145, array[1.0, 0.3, 0.0, -1.0])", "Bin values are not sorted in ascending order");

        // this is a case that we can't catch because we are using binary search to bisect the bins array
        assertFunction("width_bucket(1.5, array[1.0, 2.3, 2.0])", BIGINT, 1L);
    }

    @Test
    public void testCosineSimilarity()
    {
        assertFunction("cosine_similarity(map(array ['a', 'b'], array [1.0, 2.0]), map(array ['c', 'b'], array [1.0, 3.0]))",
                DOUBLE,
                2 * 3 / (Math.sqrt(5) * Math.sqrt(10)));

        assertFunction("cosine_similarity(map(array ['a', 'b', 'c'], array [1.0, 2.0, -1.0]), map(array ['c', 'b'], array [1.0, 3.0]))",
                DOUBLE,
                (2 * 3 + (-1) * 1) / (Math.sqrt(1 + 4 + 1) * Math.sqrt(1 + 9)));

        assertFunction("cosine_similarity(map(array ['a', 'b', 'c'], array [1.0, 2.0, -1.0]), map(array ['d', 'e'], array [1.0, 3.0]))",
                DOUBLE,
                0.0);

        assertFunction("cosine_similarity(null, map(array ['c', 'b'], array [1.0, 3.0]))",
                DOUBLE,
                null);

        assertFunction("cosine_similarity(map(array ['a', 'b'], array [DOUBLE '1.0', null]), map(array ['c', 'b'], array [DOUBLE '1.0', DOUBLE '3.0']))",
                DOUBLE,
                null);
    }
}
