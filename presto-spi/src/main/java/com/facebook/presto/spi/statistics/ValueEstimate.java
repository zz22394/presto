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

package com.facebook.presto.spi.statistics;

import static java.lang.Double.isNaN;

public final class ValueEstimate
{
    // todo eventually add some notion of statistic reliability
    //      Skipping for now as there hard to compute it properly and so far we do not have
    //      usecase for that.

    private static final double UNKNOWN_VALUE = Double.NaN;

    private final double value;

    public static final ValueEstimate unknownValue()
    {
        return new ValueEstimate(UNKNOWN_VALUE);
    }

    public ValueEstimate(double value)
    {
        this.value = value;
    }

    public boolean isValueUnknown()
    {
        return isNaN(value);
    }

    public double getValue()
    {
        return value;
    }
}
