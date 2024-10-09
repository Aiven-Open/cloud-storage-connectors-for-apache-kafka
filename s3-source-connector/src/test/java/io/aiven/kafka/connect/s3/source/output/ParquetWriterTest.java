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
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.ContentUtils;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import com.amazonaws.util.IOUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ParquetWriterTest {
    private ParquetWriter parquetWriter;
    private S3SourceConfig s3SourceConfig;
    private OffsetManager offsetManager;
    private List<ConsumerRecord<byte[], byte[]>> consumerRecordList;
    private Map<Map<String, Object>, Long> currentOffsets;
    private Map<String, Object> partitionMap;

    @BeforeEach
    public void setUp() {
        parquetWriter = new ParquetWriter();
        s3SourceConfig = mock(S3SourceConfig.class);
        offsetManager = mock(OffsetManager.class);
        consumerRecordList = new ArrayList<>();
        currentOffsets = mock(Map.class);
        partitionMap = mock(Map.class);
    }

    @Test
    void testHandleValueDataWithZeroBytes() {
        final byte[] mockParquetData = new byte[0];
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);

        final String topic = "test-topic";
        final int topicPartition = 0;
        final long startOffset = 100L;

        parquetWriter.handleValueData(Optional.empty(), inputStream, topic, consumerRecordList, s3SourceConfig,
                topicPartition, startOffset, offsetManager, currentOffsets, partitionMap);

        assertThat(consumerRecordList).isEmpty();
    }

    @Test
    void testGetRecordsWithValidData() throws Exception {
        final byte[] mockParquetData = generateMockParquetData();
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<GenericRecord> records = ParquetWriter.getRecords(inputStream, topic, topicPartition);

        assertThat(records).isNotEmpty();
        assertThat(records).extracting(record -> record.get("name").toString()).contains("name1").contains("name2");
    }

    @Test
    void testGetRecordsWithInvalidData() {
        final byte[] invalidData = "invalid data".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(invalidData);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<GenericRecord> records = ParquetWriter.getRecords(inputStream, topic, topicPartition);
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
        final Path path = ContentUtils.getTmpFilePath("name1", "name2");
        return IOUtils.toByteArray(Files.newInputStream(path));
    }
}
