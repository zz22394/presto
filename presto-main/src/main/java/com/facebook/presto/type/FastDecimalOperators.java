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

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.DecimalArithmetic;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;

public final class FastDecimalOperators
{
    private FastDecimalOperators() {}

    @ScalarOperator(ADD)
    @LiteralParameters({"p", "s"})
    @SqlType("fast_decimal(min(38, p + 1), s)")
    public static Slice add(@SqlType("fast_decimal(p, s)") Slice a, @SqlType("fast_decimal(p, s)") Slice b)
    {
        return DecimalArithmetic.add(a, b);
    }

    @ScalarOperator(MULTIPLY)
    @LiteralParameters({"a_precision", "a_scale", "b_precision", "b_scale"})
    @SqlType("fast_decimal(min(38, a_precision + b_precision), a_scale + b_scale)")
    public static Slice multiply(@SqlType("fast_decimal(a_precision, a_scale)") Slice a, @SqlType("fast_decimal(b_precision, b_scale)") Slice b)
    {
        return DecimalArithmetic.multiply(a, b);
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"p", "s"})
    @SqlType("fast_decimal(p, s)")
    public static Slice cast(@SqlType("fast_decimal(p, s)") Slice a)
    {
        return a;
    }
}
