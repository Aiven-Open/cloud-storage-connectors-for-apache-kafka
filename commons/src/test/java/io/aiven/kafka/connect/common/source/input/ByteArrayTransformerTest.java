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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

        final Stream<byte[]> records = byteArrayTransformer.getRecords(inputStreamIOSupplier, TEST_TOPIC, 0,
                sourceCommonConfig, 0);

        final List<Object> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(1);
        assertThat((byte[]) recs.get(0)).isEqualTo(data);
    }

    @Test
    void testGetRecordsEmptyInputStream() {
        final InputStream inputStream = new ByteArrayInputStream(new byte[] {});

        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;

        final Stream<byte[]> records = byteArrayTransformer.getRecords(inputStreamIOSupplier, TEST_TOPIC, 0,
                sourceCommonConfig, 0);

        assertThat(records).hasSize(0);
    }

    @Test
    void testGetValueBytes() {
        final byte[] record = { 1, 2, 3 };
        final byte[] result = (byte[]) byteArrayTransformer.getValueData(record, TEST_TOPIC, sourceCommonConfig)
                .value();

        assertThat(result).containsExactlyInAnyOrder(record);
    }
}
