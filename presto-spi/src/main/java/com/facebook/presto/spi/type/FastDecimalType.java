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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DecimalArithmetic.DECIMAL_SLICE_LENGTH;
import static com.facebook.presto.spi.type.DecimalArithmetic.MAX_PRECISION;
import static com.facebook.presto.spi.type.DecimalArithmetic.decimalToBigInteger;
import static com.facebook.presto.spi.type.StandardTypes.FAST_DECIMAL;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public class FastDecimalType
        extends AbstractFixedWidthType
{
    private static final int UNSET = -1;

    public static FastDecimalType createDecimalType(int precision, int scale)
    {
        return new FastDecimalType(precision, scale);
    }

    public static FastDecimalType createDecimalType(int precision)
    {
        return createDecimalType(precision, 0);
    }

    public static FastDecimalType createUnparametrizedDecimal()
    {
        return new FastDecimalType();
    }

    private final int precision;
    private final int scale;

    private FastDecimalType(int precision, int scale)
    {
        super(new TypeSignature(FAST_DECIMAL, buildPrecisionScaleList(precision, scale)), Slice.class, DECIMAL_SLICE_LENGTH);
        this.precision = precision;
        this.scale = scale;

        validatePrecisionScale(precision, scale);
    }

    private FastDecimalType()
    {
        super(new TypeSignature(FAST_DECIMAL, emptyList()), Slice.class, 0);
        this.precision = UNSET;
        this.scale = UNSET;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, DECIMAL_SLICE_LENGTH);
        return new SqlDecimal(decimalToBigInteger(slice), precision, scale);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, DECIMAL_SLICE_LENGTH, blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    private static List<TypeSignatureParameter> buildPrecisionScaleList(int precision, int scale)
    {
        List<TypeSignatureParameter> literalArguments = new ArrayList<>();
        literalArguments.add(TypeSignatureParameter.of((long) precision));
        literalArguments.add(TypeSignatureParameter.of((long) scale));
        return unmodifiableList(literalArguments);
    }

    private void validatePrecisionScale(int precision, int scale)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid DECIMAL precision " + precision);
        }

        if (scale < 0 || scale > precision) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid DECIMAL scale " + scale);
        }
    }
}
