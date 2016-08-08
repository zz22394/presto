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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.type.ColorType.COLOR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDomainTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private static final Symbol C_BIGINT = new Symbol("c_bigint");
    private static final Symbol C_DOUBLE = new Symbol("c_double");
    private static final Symbol C_VARCHAR = new Symbol("c_varchar");
    private static final Symbol C_BOOLEAN = new Symbol("c_boolean");
    private static final Symbol C_BIGINT_1 = new Symbol("c_bigint_1");
    private static final Symbol C_DOUBLE_1 = new Symbol("c_double_1");
    private static final Symbol C_VARCHAR_1 = new Symbol("c_varchar_1");
    private static final Symbol C_TIMESTAMP = new Symbol("c_timestamp");
    private static final Symbol C_DATE = new Symbol("c_date");
    private static final Symbol C_COLOR = new Symbol("c_color");
    private static final Symbol C_HYPER_LOG_LOG = new Symbol("c_hyper_log_log");
    private static final Symbol C_VARBINARY = new Symbol("c_varbinary");
    private static final Symbol C_DECIMAL_10_5 = new Symbol("c_decimal_10_5");
    private static final Symbol C_DECIMAL_4_2 = new Symbol("c_decimal_4_2");
    private static final Symbol C_INTEGER = new Symbol("c_integer");

    private static final Map<Symbol, Type> TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(C_BIGINT, BIGINT)
            .put(C_DOUBLE, DOUBLE)
            .put(C_VARCHAR, VARCHAR)
            .put(C_BOOLEAN, BOOLEAN)
            .put(C_BIGINT_1, BIGINT)
            .put(C_DOUBLE_1, DOUBLE)
            .put(C_VARCHAR_1, VARCHAR)
            .put(C_TIMESTAMP, TIMESTAMP)
            .put(C_DATE, DATE)
            .put(C_COLOR, COLOR) // Equatable, but not orderable
            .put(C_HYPER_LOG_LOG, HYPER_LOG_LOG) // Not Equatable or orderable
            .put(C_VARBINARY, VARBINARY)
            .put(C_DECIMAL_10_5, createDecimalType(10, 5))
            .put(C_DECIMAL_4_2, createDecimalType(4, 2))
            .put(C_INTEGER, INTEGER)
            .build();

    private static final long TIMESTAMP_VALUE = new DateTime(2013, 3, 30, 1, 5, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long DATE_VALUE = new DateTime(2001, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long COLOR_VALUE_1 = 1;
    private static final long COLOR_VALUE_2 = 2;

    @Test
    public void testNoneRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.none();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.all();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.singleValue(BOOLEAN, true))
                .put(C_BIGINT_1, Domain.singleValue(BIGINT, 2L))
                .put(C_DOUBLE_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 1.1), Range.equal(DOUBLE, 2.0), Range.range(DOUBLE, 3.0, false, 3.5, true)), true))
                .put(C_VARCHAR_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("2013-01-01")), Range.greaterThan(VARCHAR, utf8Slice("2013-10-01"))), false))
                .put(C_TIMESTAMP, Domain.singleValue(TIMESTAMP, TIMESTAMP_VALUE))
                .put(C_DATE, Domain.singleValue(DATE, DATE_VALUE))
                .put(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))
                .put(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testInOptimization()
            throws Exception
    {
        Domain testDomain = Domain.create(
                ValueSet.all(BIGINT)
                        .subtract(ValueSet.ofRanges(
                                Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))), false);

        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L)).intersect(
                        ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))));

        testDomain = Domain.create(ValueSet.ofRanges(
                Range.range(BIGINT, 1L, true, 3L, true),
                Range.range(BIGINT, 5L, true, 7L, true),
                Range.range(BIGINT, 9L, true, 11L, true)),
                false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain),
                or(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(3L)), (between(C_BIGINT, bigintLiteral(5L), bigintLiteral(7L))), (between(C_BIGINT, bigintLiteral(9L), bigintLiteral(11L)))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, true, 9L, true))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))), between(C_BIGINT, bigintLiteral(7L), bigintLiteral(9L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, false, 9L, false), Range.range(BIGINT, 11L, false, 13L, false))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(
                and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                and(greaterThan(C_BIGINT, bigintLiteral(7L)), lessThan(C_BIGINT, bigintLiteral(9L))),
                and(greaterThan(C_BIGINT, bigintLiteral(11L)), lessThan(C_BIGINT, bigintLiteral(13L)))));
    }

    @Test
    public void testToPredicateNone()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.none(BOOLEAN))
                .build());

        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);
    }

    @Test
    public void testToPredicateAllIgnored()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.all(BOOLEAN))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .build()));
    }

    @Test
    public void testToPredicate()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain;

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNotNull(C_BIGINT));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNull(C_BIGINT));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT)));
        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.all(BIGINT)));
        assertEquals(toPredicate(tupleDomain), TRUE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false)));
        assertEquals(toPredicate(tupleDomain), and(greaterThan(C_BIGINT, bigintLiteral(0L)), lessThanOrEqual(C_BIGINT, bigintLiteral(1L))));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.singleValue(BIGINT, 1L)));
        assertEquals(toPredicate(tupleDomain), equal(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));
        assertEquals(toPredicate(tupleDomain), in(C_BIGINT, ImmutableList.of(1L, 2L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true)));
        assertEquals(toPredicate(tupleDomain), or(lessThan(C_BIGINT, bigintLiteral(1L)), isNull(C_BIGINT)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), true)));
        assertEquals(toPredicate(tupleDomain), or(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));
        assertEquals(toPredicate(tupleDomain), or(not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1))), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNull(C_HYPER_LOG_LOG));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNotNull(C_HYPER_LOG_LOG));
    }

    @Test
    public void testFromUnknownPredicate()
            throws Exception
    {
        ExtractionResult result = fromPredicate(unprocessableExpression1(C_BIGINT));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));

        // Test the complement
        result = fromPredicate(not(unprocessableExpression1(C_BIGINT)));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), not(unprocessableExpression1(C_BIGINT)));
    }

    @Test
    public void testFromAndPredicate()
            throws Exception
    {
        Expression originalPredicate = and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));

        // Test complements
        originalPredicate = not(and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromOrPredicate()
            throws Exception
    {
        Expression originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // And not if they have different symbols
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), greaterThan(C_DOUBLE, doubleLiteral(1.0)), unprocessableExpression1(C_BIGINT)),
                and(greaterThan(C_BIGINT, bigintLiteral(2L)), greaterThan(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(
                C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false),
                C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false))));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), randPredicate(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), randPredicate(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(or(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));
    }

    @Test
    public void testFromNotPredicate()
            throws Exception
    {
        Expression originalPredicate = not(and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(unprocessableExpression1(C_BIGINT));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(TRUE_LITERAL);
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalPredicate = not(equal(C_BIGINT, bigintLiteral(1L)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromUnprocessableComparison()
            throws Exception
    {
        // If it is not a simple comparison, we should not try to process it
        Expression predicate = comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT));
        ExtractionResult result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());

        // Complement
        predicate = not(comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromDecimalComparison()
            throws Exception
    {
        Expression predicate = greaterThan(C_DECIMAL_10_5, decimalLiteral("12.345"));
        ExtractionResult result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_DECIMAL_10_5, Domain.create(ValueSet.ofRanges(Range.greaterThan(createDecimalType(10, 5), 1234500L)), false))));
    }

    @Test
    public void testFromBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons
        Expression originalExpression = greaterThan(C_BIGINT, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = notEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = isDistinctFrom(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = equal(C_COLOR, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        // Test complement
        originalExpression = not(greaterThan(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        originalExpression = not(isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));
    }

    @Test
    public void testFromFlippedBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        ComparisonExpression originalExpression = comparison(GREATER_THAN, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = comparison(GREATER_THAN_OR_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN_OR_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = comparison(NOT_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(NOT_EQUAL, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = comparison(IS_DISTINCT_FROM, bigintLiteral(2L), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, nullLiteral(), C_BIGINT.toSymbolReference());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
            throws Exception
    {
        // Test out the extraction of all basic comparisons with null literals
        Expression originalExpression = greaterThan(C_BIGINT, nullLiteral());
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = greaterThan(C_VARCHAR, new Cast(nullLiteral(), StandardTypes.VARCHAR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.none(VARCHAR), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThan(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThanOrEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = isDistinctFrom(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.notNull(COLOR))));

        // Test complements
        originalExpression = not(greaterThan(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(greaterThanOrEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThan(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThanOrEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(isDistinctFrom(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = not(isDistinctFrom(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.onlyNull(COLOR))));
    }

    @Test
    public void testFromComparisonsWithImplictCoercions()
            throws Exception
    {
        // B is a double column. Check that it can be compared against longs
        Expression originalExpression = greaterThan(C_DOUBLE, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = greaterThan(C_VARCHAR, stringLiteral("test"));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = greaterThan(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThan(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT))));

        originalExpression = notEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = notEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = isDistinctFrom(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        // Test complements

        // B is a double column. Check that it can be compared against longs
        originalExpression = not(greaterThan(C_DOUBLE, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = not(greaterThan(C_VARCHAR, stringLiteral("test")));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = not(greaterThan(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThan(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = not(notEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT))));

        originalExpression = not(isDistinctFrom(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromUnprocessableInPredicate()
            throws Exception
    {
        Expression originalExpression = new InPredicate(unprocessableExpression1(C_BIGINT), new InListExpression(ImmutableList.<Expression>of(TRUE_LITERAL)));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), originalExpression);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(C_BOOLEAN.toSymbolReference(), new InListExpression(ImmutableList.of(unprocessableExpression1(C_BOOLEAN))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN)));
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(C_BOOLEAN.toSymbolReference(), new InListExpression(ImmutableList.of(TRUE_LITERAL, unprocessableExpression1(C_BOOLEAN))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), or(equal(C_BOOLEAN, TRUE_LITERAL), equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN))));
        assertTrue(result.getTupleDomain().isAll());

        // Test complement
        originalExpression = not(new InPredicate(C_BOOLEAN.toSymbolReference(), new InListExpression(ImmutableList.of(unprocessableExpression1(C_BOOLEAN)))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), not(equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN))));
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromInPredicate()
            throws Exception
    {
        Expression originalExpression = in(C_BIGINT, ImmutableList.of(1L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.singleValue(BIGINT, 1L))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))));

        originalExpression = in(C_BIGINT, ImmutableList.of(1L, 2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = not(in(C_BIGINT, ImmutableList.of(1L, 2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.range(BIGINT, 1L, false, 2L, false), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        // TODO update domain translator to properly handle cast
//        originalExpression = in(A, Arrays.asList(1L, 2L, (Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(ACH, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));
//
//        originalExpression = not(in(A, Arrays.asList(1L, 2L, (Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = in(A, Arrays.asList((Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = not(in(A, Arrays.asList((Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromBetweenPredicate()
            throws Exception
    {
        Expression originalExpression = between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(C_BIGINT, bigintLiteral(1L), doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(C_BIGINT, bigintLiteral(1L), nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        // Test complements
        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromIsNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNull(C_BIGINT);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = isNull(C_HYPER_LOG_LOG);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));

        originalExpression = not(isNull(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = not(isNull(C_HYPER_LOG_LOG));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromIsNotNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNotNull(C_BIGINT);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isNotNull(C_HYPER_LOG_LOG);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));

        originalExpression = not(isNotNull(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = not(isNotNull(C_HYPER_LOG_LOG));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = TRUE_LITERAL;
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = not(TRUE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = FALSE_LITERAL;
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(FALSE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromNullLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = nullLiteral();
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testExpressionConstantFolding()
            throws Exception
    {
        Expression originalExpression = comparison(GREATER_THAN, C_VARBINARY.toSymbolReference(), function("from_hex", stringLiteral("123456")));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARBINARY, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARBINARY, value)), false))));

        Expression expression = toPredicate(result.getTupleDomain());
        assertEquals(expression, comparison(GREATER_THAN, C_VARBINARY.toSymbolReference(), varbinaryLiteral(value)));
    }

    @Test
    public void testBigintComparedToDoubleExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Range.greaterThanOrEqual(BIGINT, 2L));
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Range.greaterThan(BIGINT, -3L));
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Range.greaterThanOrEqual(BIGINT, -2L));
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Range.greaterThan(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(greaterThanOrEqual(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Range.greaterThanOrEqual(BIGINT, Long.MIN_VALUE));

        // greater than
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Range.greaterThan(BIGINT, 2L));
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Range.greaterThan(BIGINT, -3L));
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Range.greaterThan(BIGINT, -2L));
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Range.greaterThan(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(greaterThan(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Range.greaterThanOrEqual(BIGINT, Long.MIN_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Range.lessThanOrEqual(BIGINT, -3L));
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Range.lessThanOrEqual(BIGINT, -2L));
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Range.lessThanOrEqual(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(lessThanOrEqual(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Range.lessThan(BIGINT, Long.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Range.lessThanOrEqual(BIGINT, 2L));
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Range.lessThan(BIGINT, 2L));
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Range.lessThanOrEqual(BIGINT, -3L));
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Range.lessThan(BIGINT, -2L));
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Range.lessThanOrEqual(BIGINT, Long.MAX_VALUE));
        testSimpleComparison(lessThan(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Range.lessThan(BIGINT, Long.MIN_VALUE));

        // equal
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Domain.none(BIGINT));
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Range.equal(BIGINT, 2L));
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Domain.none(BIGINT));
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Range.equal(BIGINT, -2L));
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Domain.none(BIGINT));
        testSimpleComparison(equal(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Domain.none(BIGINT));

        // not equal
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false));
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, -2L), Range.greaterThan(BIGINT, -2L)), false));
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Domain.notNull(BIGINT));
        testSimpleComparison(notEqual(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Domain.notNull(BIGINT));

        // is distinct from
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(2.5)), C_BIGINT, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(2.0)), C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true));
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(-2.5)), C_BIGINT, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(-2.0)), C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, -2L), Range.greaterThan(BIGINT, -2L)), true));
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(0x1p64)), C_BIGINT, Domain.all(BIGINT));
        testSimpleComparison(isDistinctFrom(C_BIGINT, doubleLiteral(-0x1p64)), C_BIGINT, Domain.all(BIGINT));
    }

    @Test
    public void testIntegerComparedToDoubleExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(C_INTEGER, doubleLiteral(2.5)), C_INTEGER, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(C_INTEGER, doubleLiteral(2.0)), C_INTEGER, Range.greaterThanOrEqual(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(C_INTEGER, doubleLiteral(0x1p32)), C_INTEGER, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // greater than
        testSimpleComparison(greaterThan(C_INTEGER, doubleLiteral(2.5)), C_INTEGER, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(C_INTEGER, doubleLiteral(2.0)), C_INTEGER, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(C_INTEGER, doubleLiteral(0x1p32)), C_INTEGER, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(C_INTEGER, doubleLiteral(-2.5)), C_INTEGER, Range.lessThanOrEqual(INTEGER, -3L));
        testSimpleComparison(lessThanOrEqual(C_INTEGER, doubleLiteral(-2.0)), C_INTEGER, Range.lessThanOrEqual(INTEGER, -2L));
        testSimpleComparison(lessThanOrEqual(C_INTEGER, doubleLiteral(-0x1p32)), C_INTEGER, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(C_INTEGER, doubleLiteral(-2.5)), C_INTEGER, Range.lessThanOrEqual(INTEGER, -3L));
        testSimpleComparison(lessThan(C_INTEGER, doubleLiteral(-2.0)), C_INTEGER, Range.lessThan(INTEGER, -2L));
        testSimpleComparison(lessThan(C_INTEGER, doubleLiteral(-0x1p32)), C_INTEGER, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // equal
        testSimpleComparison(equal(C_INTEGER, doubleLiteral(2.5)), C_INTEGER, Domain.none(INTEGER));
        testSimpleComparison(equal(C_INTEGER, doubleLiteral(2.0)), C_INTEGER, Range.equal(INTEGER, 2L));

        // not equal
        testSimpleComparison(notEqual(C_INTEGER, doubleLiteral(2.5)), C_INTEGER, Domain.notNull(INTEGER));
        testSimpleComparison(notEqual(C_INTEGER, doubleLiteral(2.0)), C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(C_INTEGER, doubleLiteral(2.5)), C_INTEGER, Domain.all(INTEGER));
        testSimpleComparison(isDistinctFrom(C_INTEGER, doubleLiteral(2.0)), C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), true));
    }

    @Test
    public void testIntegerComparedToBigintExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(C_INTEGER, bigintLiteral(2L)), C_INTEGER, Range.greaterThanOrEqual(INTEGER, 2L));
        testSimpleComparison(greaterThanOrEqual(C_INTEGER, bigintLiteral(Integer.MAX_VALUE + 1L)), C_INTEGER, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // greater than
        testSimpleComparison(greaterThan(C_INTEGER, bigintLiteral(2L)), C_INTEGER, Range.greaterThan(INTEGER, 2L));
        testSimpleComparison(greaterThan(C_INTEGER, bigintLiteral(Integer.MAX_VALUE + 1L)), C_INTEGER, Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(C_INTEGER, bigintLiteral(-2L)), C_INTEGER, Range.lessThanOrEqual(INTEGER, -2L));
        testSimpleComparison(lessThanOrEqual(C_INTEGER, bigintLiteral(Integer.MIN_VALUE - 1L)), C_INTEGER, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // less than
        testSimpleComparison(lessThan(C_INTEGER, bigintLiteral(-2L)), C_INTEGER, Range.lessThan(INTEGER, -2L));
        testSimpleComparison(lessThan(C_INTEGER, bigintLiteral(Integer.MIN_VALUE - 1L)), C_INTEGER, Range.lessThan(INTEGER, (long) Integer.MIN_VALUE));

        // equal
        testSimpleComparison(equal(C_INTEGER, bigintLiteral(Integer.MIN_VALUE - 1L)), C_INTEGER, Domain.none(INTEGER));
        testSimpleComparison(equal(C_INTEGER, bigintLiteral(-2L)), C_INTEGER, Range.equal(INTEGER, -2L));

        // not equal
        testSimpleComparison(notEqual(C_INTEGER, bigintLiteral(Integer.MAX_VALUE + 1L)), C_INTEGER, Domain.notNull(INTEGER));
        testSimpleComparison(notEqual(C_INTEGER, bigintLiteral(2L)), C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(C_INTEGER, bigintLiteral(Integer.MAX_VALUE + 1L)), C_INTEGER, Domain.all(INTEGER));
        testSimpleComparison(isDistinctFrom(C_INTEGER, bigintLiteral(2L)), C_INTEGER, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 2L), Range.greaterThan(INTEGER, 2L)), true));
    }

    @Test
    public void testDecimalComparedToWiderDecimal()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(C_DECIMAL_4_2, decimalLiteral("44.555678")), C_DECIMAL_4_2, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThanOrEqual(C_DECIMAL_4_2, decimalLiteral("99.99")), C_DECIMAL_4_2, Range.greaterThanOrEqual(createDecimalType(4, 2), shortDecimal("99.99")));
        testSimpleComparison(greaterThanOrEqual(C_DECIMAL_4_2, decimalLiteral("9999.999")), C_DECIMAL_4_2, Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99")));

        // greater than
        testSimpleComparison(greaterThan(C_DECIMAL_4_2, decimalLiteral("44.555678")), C_DECIMAL_4_2, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThan(C_DECIMAL_4_2, decimalLiteral("44.55")), C_DECIMAL_4_2, Range.greaterThan(createDecimalType(4, 2), shortDecimal("44.55")));
        testSimpleComparison(greaterThan(C_DECIMAL_4_2, decimalLiteral("9999.999")), C_DECIMAL_4_2, Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99")));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(C_DECIMAL_4_2, decimalLiteral("-44.555678")), C_DECIMAL_4_2, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-44.56")));
        testSimpleComparison(lessThanOrEqual(C_DECIMAL_4_2, decimalLiteral("-99.99")), C_DECIMAL_4_2, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-99.99")));
        testSimpleComparison(lessThanOrEqual(C_DECIMAL_4_2, decimalLiteral("-9999.999")), C_DECIMAL_4_2, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));

        // less than
        testSimpleComparison(lessThan(C_DECIMAL_4_2, decimalLiteral("-44.555678")), C_DECIMAL_4_2, Range.lessThanOrEqual(createDecimalType(4, 2), shortDecimal("-44.56")));
        testSimpleComparison(lessThan(C_DECIMAL_4_2, decimalLiteral("-99.99")), C_DECIMAL_4_2, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));
        testSimpleComparison(lessThan(C_DECIMAL_4_2, decimalLiteral("-9999.999")), C_DECIMAL_4_2, Range.lessThan(createDecimalType(4, 2), shortDecimal("-99.99")));

        // equal
        testSimpleComparison(equal(C_DECIMAL_4_2, decimalLiteral("-44.555678")), C_DECIMAL_4_2, Domain.none(createDecimalType(4, 2)));
        testSimpleComparison(equal(C_DECIMAL_4_2, decimalLiteral("99.99")), C_DECIMAL_4_2, Range.equal(createDecimalType(4, 2), shortDecimal("99.99")));

        // not equal
        testSimpleComparison(notEqual(C_DECIMAL_4_2, decimalLiteral("-44.555678")), C_DECIMAL_4_2, Domain.notNull(createDecimalType(4, 2)));
        testSimpleComparison(notEqual(C_DECIMAL_4_2, decimalLiteral("99.99")), C_DECIMAL_4_2, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createDecimalType(4, 2), shortDecimal("99.99")), Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99"))), false));

        // is distinct from
        testSimpleComparison(isDistinctFrom(C_DECIMAL_4_2, decimalLiteral("-44.555678")), C_DECIMAL_4_2, Domain.all(createDecimalType(4, 2)));
        testSimpleComparison(isDistinctFrom(C_DECIMAL_4_2, decimalLiteral("99.99")), C_DECIMAL_4_2, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createDecimalType(4, 2), shortDecimal("99.99")), Range.greaterThan(createDecimalType(4, 2), shortDecimal("99.99"))), true));
    }

    private static ExtractionResult fromPredicate(Expression originalPredicate)
    {
        return DomainTranslator.fromPredicate(METADATA, TEST_SESSION, originalPredicate, TYPES);
    }

    private static Expression toPredicate(TupleDomain<Symbol> tupleDomain)
    {
        return DomainTranslator.toPredicate(tupleDomain);
    }

    private static Expression unprocessableExpression1(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private static Expression unprocessableExpression2(Symbol symbol)
    {
        return comparison(LESS_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private static Expression randPredicate(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of()));
    }

    private static NotExpression not(Expression expression)
    {
        return new NotExpression(expression);
    }

    private static ComparisonExpression comparison(ComparisonExpression.Type type, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(type, expression1, expression2);
    }

    private static ComparisonExpression equal(Symbol symbol, Expression expression)
    {
        return comparison(EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression notEqual(Symbol symbol, Expression expression)
    {
        return comparison(NOT_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression greaterThan(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN_OR_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression lessThan(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN_OR_EQUAL, symbol.toSymbolReference(), expression);
    }

    private static ComparisonExpression isDistinctFrom(Symbol symbol, Expression expression)
    {
        return comparison(IS_DISTINCT_FROM, symbol.toSymbolReference(), expression);
    }

    private static Expression isNotNull(Symbol symbol)
    {
        return new NotExpression(new IsNullPredicate(symbol.toSymbolReference()));
    }

    private static IsNullPredicate isNull(Symbol symbol)
    {
        return new IsNullPredicate(symbol.toSymbolReference());
    }

    private static InPredicate in(Symbol symbol, List<?> values)
    {
        List<Type> types = nCopies(values.size(), TYPES.get(symbol));
        List<Expression> expressions = LiteralInterpreter.toExpressions(values, types);
        return new InPredicate(symbol.toSymbolReference(), new InListExpression(expressions));
    }

    private static BetweenPredicate between(Symbol symbol, Expression min, Expression max)
    {
        return new BetweenPredicate(symbol.toSymbolReference(), min, max);
    }

    private static Literal bigintLiteral(long value)
    {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new GenericLiteral("BIGINT", Long.toString(value));
        }
        return new LongLiteral(Long.toString(value));
    }

    private static DoubleLiteral doubleLiteral(double value)
    {
        return new DoubleLiteral(Double.toString(value));
    }

    private static DecimalLiteral decimalLiteral(String value)
    {
        return new DecimalLiteral(value);
    }

    private static StringLiteral stringLiteral(String value)
    {
        return new StringLiteral(value);
    }

    private static NullLiteral nullLiteral()
    {
        return new NullLiteral();
    }

    private static FunctionCall colorLiteral(long value)
    {
        return new FunctionCall(QualifiedName.of(getMagicLiteralFunctionSignature(COLOR).getName()), ImmutableList.<Expression>of(bigintLiteral(value)));
    }

    private static Expression varbinaryLiteral(Slice value)
    {
        return LiteralInterpreter.toExpression(value, VARBINARY);
    }

    private static FunctionCall function(String functionName, Expression... args)
    {
        return new FunctionCall(QualifiedName.of(functionName), ImmutableList.copyOf(args));
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Range expectedDomainRange)
    {
        testSimpleComparison(expression, symbol, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Domain domain)
    {
        ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(symbol, domain)));
    }
}
