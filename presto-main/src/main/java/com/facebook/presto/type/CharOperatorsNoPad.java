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
package com.facebook.presto.type;

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;

public final class CharOperatorsNoPad
{
    private CharOperatorsNoPad() {}

    @LiteralParameters({"x", "y"})
    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return left.equals(right) && leftTypeLength.equals(rightTypeLength);
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return !equal(leftTypeLength, rightTypeLength, left, right);
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) < 0;
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) <= 0;
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) > 0;
    }

    @LiteralParameters({"x", "y"})
    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @LiteralParameter("x") Long leftTypeLength, @LiteralParameter("y") Long rightTypeLength,
            @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) >= 0;
    }

    @LiteralParameters({"x", "y", "z"})
    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @LiteralParameter("x") Long valueTypeLength, @LiteralParameter("y") Long minTypeLength, @LiteralParameter("z") Long maxTypeLength,
            @SqlType("char(x)") Slice value, @SqlType("char(y)") Slice min, @SqlType("char(z)") Slice max)
    {
        return compareNoPad(min, value, minTypeLength, valueTypeLength) <= 0
                && compareNoPad(value, max, valueTypeLength, maxTypeLength) <= 0;
    }

    private static int compareNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        int compareResult = left.compareTo(right);
        if (compareResult != 0) {
            return compareResult;
        }

        return (int) (leftTypeLength - rightTypeLength);
    }

    @LiteralParameters("x")
    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("char(x)") Slice value)
    {
        return XxHash64.hash(value);
    }
}
