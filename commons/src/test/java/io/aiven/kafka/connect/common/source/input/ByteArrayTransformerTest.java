/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.common.source.input;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.io.function.IOSupplier;
import org.apache.http.util.ByteArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class ByteArrayTransformerTest {

    public static final String TEST_TOPIC = "test-topic";
    private ByteArrayTransformer byteArrayTransformer;

    @Mock
    private SourceCommonConfig sourceCommonConfig;

    @BeforeEach
    void setUp() {
        byteArrayTransformer = new ByteArrayTransformer();
    }

    @Test
    void testGetRecordsSingleChunk() {
        final byte[] data = { 1, 2, 3, 4, 5 };
        final InputStream inputStream = new ByteArrayInputStream(data);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;
        when(sourceCommonConfig.getByteArrayTransformerMaxBufferSize()).thenReturn(4096);
        final Stream<SchemaAndValue> records = byteArrayTransformer.getRecords(inputStreamIOSupplier, TEST_TOPIC, 0,
                sourceCommonConfig, 0);

        final List<SchemaAndValue> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(1);
        assertThat(recs.get(0).value()).isEqualTo(data);
    }

    @Test
    void testGetRecordsEmptyInputStream() {
        final InputStream inputStream = new ByteArrayInputStream(new byte[] {});

        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;

        final Stream<SchemaAndValue> records = byteArrayTransformer.getRecords(inputStreamIOSupplier, TEST_TOPIC, 0,
                sourceCommonConfig, 0);

        assertThat(records).hasSize(0);
    }

    /**
     * @param maxBufferSize
     *            the maximum buffer size
     * @param numberOfExpectedRecords
     *            the number of records the byte array is split into based off the max buffer size
     */
    @ParameterizedTest
    @CsvSource({ "1,10", "2,5", "3,4", "4,3", "5,2", "6,2", "7,2", "8,2", "9,2", "10,1", "11,1", "12,1" })
    void testGetRecordsWithVariableMaxBufferSize(final int maxBufferSize, final int numberOfExpectedRecords) {
        final byte[] data = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        final InputStream inputStream = new ByteArrayInputStream(data);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;
        when(sourceCommonConfig.getByteArrayTransformerMaxBufferSize()).thenReturn(maxBufferSize);

        final Stream<SchemaAndValue> records = byteArrayTransformer.getRecords(inputStreamIOSupplier, TEST_TOPIC, 0,
                sourceCommonConfig, 0);

        final List<SchemaAndValue> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(numberOfExpectedRecords);
        final ByteArrayBuffer processedData = new ByteArrayBuffer(10);

        recs.forEach(rec -> {
            final byte[] val = (byte[]) rec.value();
            processedData.append(val, 0, val.length);
        });
        assertThat(processedData.buffer()).isEqualTo(data);
        // Should only get called once per splitIterator
        verify(sourceCommonConfig, times(1)).getByteArrayTransformerMaxBufferSize();
    }
}
