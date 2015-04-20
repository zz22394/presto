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
package com.teradata.presto.functions.dateformat;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestDateFormatLexer
{
    private class SimpleToken implements DateFormatLexer.Token
    {
        private String token;

        private SimpleToken(String token)
        {
            this.token = token;
        }

        @Override
        public String representation()
        {
            return token;
        }
    }

    private DateFormatLexer yearLexer;

    @BeforeClass
    public void setUp()
    {
        yearLexer = DateFormatLexer.builder().addToken(new SimpleToken("yyyy")).build();
    }

    @Test
    public void testSimpleFormat() throws ParseException
    {
        List<DateFormatLexer.Token> tokens = yearLexer.tokenize("yyyy");
        assertEquals(tokens.size(), 1);
        assertEquals(tokens.get(0).representation(), "yyyy");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testInvalidToken1() throws ParseException
    {
        yearLexer.tokenize("yyy");
    }

    @Test(expectedExceptions = ParseException.class)
    public void testInvalidToken2() throws ParseException
    {
        yearLexer.tokenize("ala");
    }

    @Test
    public void testYearMonthDay() throws ParseException
    {
        DateFormatLexer yearMonthDayLexer = DateFormatLexer.builder()
                .addToken(new SimpleToken("yyyy"))
                .addToken(new SimpleToken("mm"))
                .addToken(new SimpleToken("dd"))
                .addToken(new SimpleToken("-"))
                .addToken(new SimpleToken("/"))
                .build();

        List<DateFormatLexer.Token> tokens = yearMonthDayLexer.tokenize("mm-dd/yyyy");
        assertEquals(tokens.size(), 5);
        assertEquals(tokens.get(0).representation(), "mm");
        assertEquals(tokens.get(1).representation(), "-");
        assertEquals(tokens.get(2).representation(), "dd");
        assertEquals(tokens.get(3).representation(), "/");
        assertEquals(tokens.get(4).representation(), "yyyy");
    }

    @Test
    public void testGreedinessLongFirst() throws ParseException
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .addToken(new SimpleToken("yyy"))
                .addToken(new SimpleToken("yy"))
                .addToken(new SimpleToken("y"))
                .build();

        assertEquals(lexer.tokenize("y").size(), 1);
        assertEquals(lexer.tokenize("yy").size(), 1);
        assertEquals(lexer.tokenize("yyy").size(), 1);
        assertEquals(lexer.tokenize("yyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyyy").size(), 2);
        assertEquals(lexer.tokenize("yyyyyyy").size(), 3);
    }

    @Test
    public void testGreedinessShortFirst() throws ParseException
    {
        DateFormatLexer lexer = DateFormatLexer.builder()
                .addToken(new SimpleToken("y"))
                .addToken(new SimpleToken("yy"))
                .addToken(new SimpleToken("yyy"))
                .build();

        assertEquals(lexer.tokenize("y").size(), 1);
        assertEquals(lexer.tokenize("yy").size(), 2);
        assertEquals(lexer.tokenize("yyy").size(), 3);
    }
}
