/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.collections.RingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RingBufferTest {

    public static final String OBJECT_KEY = "S3ObjectKey";

    @ParameterizedTest
    @CsvSource({ "2", "10", "19", "24" })
    void testRingBufferReturnsOldestEntryAndRemovesOldestEntry(final int size) {

        final RingBuffer<String> buffer = new RingBuffer<>(size);
        for (int i = 0; i < size; i++) {
            buffer.add(OBJECT_KEY + i);
        }
        assertThat(buffer.getNextEjected()).isEqualTo("S3ObjectKey" + 0);
        // Add one more unique ObjectKey
        buffer.add(OBJECT_KEY);
        assertThat(buffer.getNextEjected()).isEqualTo("S3ObjectKey" + 1);
    }

    @ParameterizedTest
    @CsvSource({ "2", "10", "19", "24" })
    void testRingBufferOnlyAddsEachItemOnce(final int size) {

        final RingBuffer<String> buffer = new RingBuffer<>(size);
        for (int i = 0; i < size; i++) {
            // add the same objectKey every time, it should onl have one entry.
            buffer.add(OBJECT_KEY);
        }
        // Buffer not filled so should return null
        assertThat(buffer.getNextEjected()).isEqualTo(null);
        assertThat(buffer.head()).isEqualTo(OBJECT_KEY);
        assertThat(buffer.contains(OBJECT_KEY)).isTrue();
    }

    @Test
    void testRingBufferOfSizeOneOnlyRetainsOneEntry() {

        final RingBuffer<String> buffer = new RingBuffer<>(1);
        buffer.add(OBJECT_KEY + 0);
        assertThat(buffer.getNextEjected()).isEqualTo(OBJECT_KEY + 0);
        buffer.add(OBJECT_KEY + 1);
        assertThat(buffer.getNextEjected()).isEqualTo(OBJECT_KEY + 1);
    }
}
