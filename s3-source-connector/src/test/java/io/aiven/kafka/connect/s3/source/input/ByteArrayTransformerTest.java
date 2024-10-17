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

package io.aiven.kafka.connect.s3.source.input;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_MESSAGE_BYTES_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class ByteArrayTransformerTest {

    private ByteArrayTransformer byteArrayTransformer;

    @Mock
    private S3SourceConfig s3SourceConfig;

    @BeforeEach
    void setUp() {
        byteArrayTransformer = new ByteArrayTransformer();
    }

    @Test
    void testGetRecordsSingleChunk() {
        final byte[] data = { 1, 2, 3, 4, 5 };
        final InputStream inputStream = new ByteArrayInputStream(data);

        when(s3SourceConfig.getInt(MAX_MESSAGE_BYTES_SIZE)).thenReturn(10_000); // Larger than data size

        final List<Object> records = byteArrayTransformer.getRecords(inputStream, "test-topic", 0, s3SourceConfig);

        assertEquals(1, records.size());
        assertArrayEquals(data, (byte[]) records.get(0));
    }

    @Test
    void testGetRecordsMultipleChunks() {
        final byte[] data = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        final InputStream inputStream = new ByteArrayInputStream(data);

        when(s3SourceConfig.getInt(MAX_MESSAGE_BYTES_SIZE)).thenReturn(5); // Smaller than data size

        final List<Object> records = byteArrayTransformer.getRecords(inputStream, "test-topic", 0, s3SourceConfig);

        assertEquals(2, records.size());
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, (byte[]) records.get(0));
        assertArrayEquals(new byte[] { 6, 7, 8, 9, 10 }, (byte[]) records.get(1));
    }

    @Test
    void testGetRecordsEmptyInputStream() throws IOException {
        final InputStream inputStream = new ByteArrayInputStream(new byte[] {});

        when(s3SourceConfig.getInt(MAX_MESSAGE_BYTES_SIZE)).thenReturn(5);

        final List<Object> records = byteArrayTransformer.getRecords(inputStream, "test-topic", 0, s3SourceConfig);

        assertEquals(0, records.size());
    }

    @Test
    void testGetValueBytes() {
        final byte[] record = { 1, 2, 3 };
        final byte[] result = byteArrayTransformer.getValueBytes(record, "test-topic", s3SourceConfig);

        assertArrayEquals(record, result);
    }
}
