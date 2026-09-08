/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTimeRaw;
import static org.neo4j.values.storable.LocalDateTimeValue.parse;
import static org.neo4j.values.storable.LocalDateTimeValue.parsePattern;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

class LocalDateTimeValueTest {
    @Test
    void shouldParseDate() {
        assertEquals(
                localDateTime(date(2017, 12, 17), localTime(17, 14, 35, 123456789)),
                parse("2017-12-17T17:14:35.123456789"));
    }

    @Test
    void shouldWriteDateTime() {
        // given
        for (LocalDateTimeValue value : new LocalDateTimeValue[] {
            localDateTime(date(2017, 3, 26), localTime(1, 0, 0, 0)),
            localDateTime(date(2017, 3, 26), localTime(2, 0, 0, 0)),
            localDateTime(date(2017, 3, 26), localTime(3, 0, 0, 0)),
            localDateTime(date(2017, 10, 29), localTime(2, 0, 0, 0)),
            localDateTime(date(2017, 10, 29), localTime(3, 0, 0, 0)),
            localDateTime(date(2017, 10, 29), localTime(4, 0, 0, 0)),
        }) {
            List<LocalDateTimeValue> values = new ArrayList<>(1);
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly() {
                @Override
                public void writeLocalDateTime(LocalDateTime localDateTime) {
                    values.add(localDateTime(localDateTime));
                }
            };

            // when
            value.writeTo(writer);

            // then
            assertEquals(singletonList(value), values);
        }
    }

    @Test
    void shouldFailOnInvalidRawValue() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> localDateTimeRaw(31556889864403200L, 0))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Instant exceeds minimum or maximum instant")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22007)
                .hasStatusDescription("error: data exception - invalid date, time, or datetime format")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N11)
                .hasStatusDescription(
                        "error: data exception - invalid argument. Invalid argument: cannot process 'epochSecond'.");
    }

    @Test
    void shouldFailOnOverflowWhenAddingDurationToLocalDateTimes() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> localDateTime(date(+999999999, 10, 29), localTime(0, 0, 0, 0))
                                .add(DurationValue.duration(8, 7, 87, 0)))
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003)
                .hasStatusDescription(
                        "error: data exception - numeric value out of range. The numeric value +999999999-10-29T00:00 + P8M7DT1M27S is outside the required range.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N28)
                .hasStatusDescription(
                        "error: data exception - overflow error. The result of the operation '+' has caused an overflow.");
    }

    @Test
    void shouldFailOnOverflowWhenSubtractionDurationFromLocalDateTimes() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> localDateTime(date(-999999999, 1, 1), localTime(1, 0, 0, 0))
                                .sub(DurationValue.duration(0, 0, 3700, 0)))
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003)
                .hasStatusDescription(
                        "error: data exception - numeric value out of range. The numeric value -999999999-01-01T01:00 - PT1H1M40S is outside the required range.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N28)
                .hasStatusDescription(
                        "error: data exception - overflow error. The result of the operation '-' has caused an overflow.");
    }

    @Test
    void shouldEqualItself() {
        assertEqual(localDateTime(2018, 1, 31, 10, 52, 5, 6), localDateTime(2018, 1, 31, 10, 52, 5, 6));
    }

    @Test
    void shouldNotEqualOther() {
        assertNotEqual(localDateTime(2018, 1, 31, 10, 52, 5, 6), localDateTime(2018, 1, 31, 10, 52, 5, 7));
    }

    @Test
    void shouldParsePatternWithLiteral() {
        assertEquals(
                localDateTime(date(2024, 6, 27), localTime(14, 30, 0, 0)),
                parsePattern(stringValue("2024-06-27 14:30"), stringValue("yyyy-MM-dd HH:mm")));
        assertEquals(
                localDateTime(date(2024, 6, 27), localTime(14, 30, 0, 0)),
                parsePattern(stringValue("2024-06-27 14:30 CEST"), stringValue("yyyy-MM-dd HH:mm 'CEST'")));
    }

    @Test
    void shouldNotParsePatternWhenLiteralDoesNotMatchInput() {
        assertThrows(
                TemporalParseException.class,
                () -> parsePattern(stringValue("2024-06-27 14.30"), stringValue("yyyy-MM-dd HH:mm")));
        assertThrows(
                TemporalParseException.class,
                () -> parsePattern(stringValue("2024-06-27 14:30 UTC"), stringValue("yyyy-MM-dd HH:mm 'CEST'")));
        assertThrows(
                TemporalParseException.class,
                () -> parsePattern(stringValue("2024-06-27 14:30"), stringValue("yyyy-MM-dd HH:mm 'CEST'")));
    }
}
