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

package io.aiven.kafka.connect.s3.source.output;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import io.aiven.kafka.connect.s3.source.testutils.ContentUtils;

import com.amazonaws.util.IOUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ParquetWriterTest {
    private ParquetWriter parquetWriter;

    @BeforeEach
    public void setUp() {
        parquetWriter = new ParquetWriter();
    }

    @Test
    void testHandleValueDataWithZeroBytes() {
        final byte[] mockParquetData = new byte[0];
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);

        final String topic = "test-topic";
        final int topicPartition = 0;
        final List<Object> recs = parquetWriter.getRecords(inputStream, topic, topicPartition);

        assertThat(recs).isEmpty();
    }

    @Test
    void testGetRecordsWithValidData() throws Exception {
        final byte[] mockParquetData = generateMockParquetData();
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<Object> records = parquetWriter.getRecords(inputStream, topic, topicPartition);

        assertThat(records).isNotEmpty();
        assertThat(records).extracting(record -> ((GenericRecord) record).get("name").toString())
                .contains("name1")
                .contains("name2");
    }

    @Test
    void testGetRecordsWithInvalidData() {
        final byte[] invalidData = "invalid data".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(invalidData);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<Object> records = parquetWriter.getRecords(inputStream, topic, topicPartition);
        assertThat(records).isEmpty();
    }

    @Test
    void testTemporaryFileDeletion() throws Exception {
        final Path tempFile = Files.createTempFile("test-file", ".parquet");
        assertThat(Files.exists(tempFile)).isTrue();

        ParquetWriter.deleteTmpFile(tempFile);
        assertThat(Files.exists(tempFile)).isFalse();
    }

    private byte[] generateMockParquetData() throws IOException {
        final Path path = ContentUtils.getTmpFilePath("name");
        return IOUtils.toByteArray(Files.newInputStream(path));
    }
}
