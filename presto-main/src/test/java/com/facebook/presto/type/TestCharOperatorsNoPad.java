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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class TestCharOperatorsNoPad
        extends AbstractTestFunctions
{
    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) = cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo  ' as char(5)) = cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) = cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) = cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) = cast('foo' as char(3))", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) <> cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo  ' as char(5)) <> cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) <> cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) <> cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) <> cast('foo' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) < cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) < cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) < cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) < cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) < cast('foo' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) <= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) <= cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) <= cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) <= cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) <= cast('foo' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) > cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) > cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) > cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) > cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) > cast('foo' as char(3))", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) >= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) >= cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) >= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) >= cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) >= cast('foo' as char(3))", BOOLEAN, false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("cast('foo' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo ' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('foo  ' as char(3))", BOOLEAN, true);

        assertFunction("cast('foo' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);

        assertFunction("cast('bar' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);

        assertFunction("cast('bar' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(3))", BOOLEAN, true);

        assertFunction("cast('bar' as char(4)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(5))", BOOLEAN, true);
        assertFunction("cast('bar' as char(4)) BETWEEN cast('bar' as char(5)) AND cast('bar' as char(7))", BOOLEAN, false);
    }
}
