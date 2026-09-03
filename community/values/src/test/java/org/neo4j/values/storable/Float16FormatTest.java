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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Float16Format.BFLOAT16;
import static org.neo4j.values.storable.Float16Format.FLOAT16;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class Float16FormatTest {
    /** Values that both float16 and bfloat16 can hold without any rounding. */
    private static final float[] EXACTLY_REPRESENTABLE_VALUES = {0.5f, 1, 1.5f, 2, 10.25f, 100, 1024};

    @RepeatedTest(100)
    void shouldConvertFloat32ToFloat16() {
        // given
        float float32Value = ThreadLocalRandom.current().nextFloat();

        // when
        short float16Value = FLOAT16.toFloat16(float32Value);
        assertThat(FLOAT16.isFinite(float16Value)).isTrue();
        float backToFloat32 = FLOAT16.toFloat32(float16Value);

        // then
        assertThat(backToFloat32).isCloseTo(float32Value, Percentage.withPercentage(10));
    }

    @RepeatedTest(100)
    void shouldConvertFloat32ToBFloat16() {
        // given
        float float32Value = ThreadLocalRandom.current().nextFloat();

        // when
        short float16Value = BFLOAT16.toFloat16(float32Value);
        assertThat(BFLOAT16.isFinite(float16Value)).isTrue();
        float backToFloat32 = BFLOAT16.toFloat32(float16Value);

        // then
        assertThat(backToFloat32).isCloseTo(float32Value, Percentage.withPercentage(1));
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldRepresentPositiveInfinity(Float16Format format) {
        // given
        float value = Float.POSITIVE_INFINITY;

        // when
        short float16 = format.toFloat16(value);

        assertThat(format.isFinite(float16)).isFalse();

        // then
        assertThat(format.toFloat32(float16)).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldRepresentNegativeInfinity(Float16Format format) {
        // given
        float value = Float.NEGATIVE_INFINITY;

        // when
        short float16 = format.toFloat16(value);

        assertThat(format.isFinite(float16)).isFalse();

        // then
        assertThat(format.toFloat32(float16)).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldRepresentNan(Float16Format format) {
        // given
        float value = Float.NaN;

        // when
        short float16 = format.toFloat16(value);

        // then
        assertThat(format.toFloat32(float16)).isNaN();
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldMakeValuesNegative(Float16Format format) {
        for (float value : EXACTLY_REPRESENTABLE_VALUES) {
            // given
            short positive = format.toFloat16(value);
            short alreadyNegative = format.toFloat16(-value);

            // when/then
            assertThat(format.toFloat32(format.negative(positive))).isEqualTo(-value);
            assertThat(format.negative(alreadyNegative)).isEqualTo(alreadyNegative);
        }
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldMakeValuesPositive(Float16Format format) {
        for (float value : EXACTLY_REPRESENTABLE_VALUES) {
            // given
            short negative = format.toFloat16(-value);
            short alreadyPositive = format.toFloat16(value);

            // when/then
            assertThat(format.toFloat32(format.positive(negative))).isEqualTo(value);
            assertThat(format.positive(alreadyPositive)).isEqualTo(alreadyPositive);
        }
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldFlipSignOfZero(Float16Format format) {
        // given
        short positiveZero = format.toFloat16(0f);
        short negativeZero = format.toFloat16(-0f);

        // when/then
        assertThat(format.negative(positiveZero)).isEqualTo(negativeZero);
        assertThat(format.positive(negativeZero)).isEqualTo(positiveZero);
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldFlipSignOfInfinity(Float16Format format) {
        // given
        short positiveInfinity = format.toFloat16(Float.POSITIVE_INFINITY);
        short negativeInfinity = format.toFloat16(Float.NEGATIVE_INFINITY);

        // when/then
        assertThat(format.negative(positiveInfinity)).isEqualTo(negativeInfinity);
        assertThat(format.positive(negativeInfinity)).isEqualTo(positiveInfinity);
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldCompareValuesInAscendingOrder(Float16Format format) {
        // given
        float[] ascending = {
            Float.NEGATIVE_INFINITY, -1024, -1, -0.5f, 0, 0.5f, 1, 1024, Float.POSITIVE_INFINITY,
        };
        short[] float16s = format.toFloat16(ascending);

        // when/then
        for (int i = 0; i < float16s.length; i++) {
            for (int j = 0; j < float16s.length; j++) {
                int comparison = format.compare(float16s[i], float16s[j]);
                assertThat(Integer.signum(comparison))
                        .as("compare(%s, %s)", ascending[i], ascending[j])
                        .isEqualTo(Integer.signum(Integer.compare(i, j)));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldCompareConsistentlyWithFloat32(Float16Format format) {
        // given
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 1_000; i++) {
            short float16Value1 = randomFinite(format, random);
            short float16Value2 = randomFinite(format, random);

            // when
            int comparison = format.compare(float16Value1, float16Value2);

            // then
            int expected = Float.compare(format.toFloat32(float16Value1), format.toFloat32(float16Value2));
            assertThat(Integer.signum(comparison))
                    .as("compare(%s, %s)", format.toFloat32(float16Value1), format.toFloat32(float16Value2))
                    .isEqualTo(Integer.signum(expected));
        }
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldCompareMinAndMaxValue(Float16Format format) {
        // given
        short min = format.minValue();
        short max = format.maxValue();

        // when/then
        assertThat(format.compare(min, max)).isNegative();
        assertThat(format.compare(max, min)).isPositive();
        assertThat(format.compare(min, min)).isZero();
        assertThat(format.compare(format.negative(max), min)).isNegative();
    }

    @ParameterizedTest
    @MethodSource("formats")
    void shouldGetCoordinates(Float16Format format) {
        // when
        AbstractFloat16Vector vector = format.instantiateVectorValue(format.toFloat16(EXACTLY_REPRESENTABLE_VALUES));

        // then
        assertThat(vector.dimensions()).isEqualTo(EXACTLY_REPRESENTABLE_VALUES.length);
        for (int i = 0; i < vector.dimensions(); i++) {
            assertThat(vector.floatValue(i)).isEqualTo(EXACTLY_REPRESENTABLE_VALUES[i]);
        }
    }

    private static short randomFinite(Float16Format format, ThreadLocalRandom random) {
        short value;
        do {
            value = (short) random.nextInt();
        } while (!format.isFinite(value));
        return value;
    }

    private static Stream<Arguments> formats() {
        return Arrays.stream(Float16Format.values()).map(Arguments::of);
    }
}
