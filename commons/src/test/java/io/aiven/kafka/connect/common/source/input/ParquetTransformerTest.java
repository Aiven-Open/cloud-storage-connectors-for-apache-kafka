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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class ParquetTransformerTest {
    private ParquetTransformer parquetTransformer;

    @BeforeEach
    public void setUp() {
        parquetTransformer = new ParquetTransformer(new AvroData(100));
    }

    @Test
    void testHandleValueDataWithZeroBytes() {
        final byte[] mockParquetData = new byte[0];
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;
        final SourceCommonConfig s3SourceConfig = mock(SourceCommonConfig.class);

        final String topic = "test-topic";
        final int topicPartition = 0;
        final Stream<GenericRecord> recs = parquetTransformer.getRecords(inputStreamIOSupplier, topic, topicPartition,
                s3SourceConfig, 0L);

        assertThat(recs).isEmpty();
    }

    @Test
    void testGetRecordsWithValidData() throws Exception {
        final byte[] mockParquetData = generateMockParquetData();
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;
        final SourceCommonConfig s3SourceConfig = mock(SourceCommonConfig.class);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<Object> records = parquetTransformer
                .getRecords(inputStreamIOSupplier, topic, topicPartition, s3SourceConfig, 0L)
                .collect(Collectors.toList());

        assertThat(records).hasSize(100);
        assertThat(records).extracting(record -> ((GenericRecord) record).get("name").toString())
                .contains("name1")
                .contains("name2");
    }

    @Test
    void testGetRecordsWithValidDataSkipFew() throws Exception {
        final byte[] mockParquetData = generateMockParquetData();
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;
        final SourceCommonConfig s3SourceConfig = mock(SourceCommonConfig.class);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final List<Object> records = parquetTransformer
                .getRecords(inputStreamIOSupplier, topic, topicPartition, s3SourceConfig, 25L)
                .collect(Collectors.toList());

        assertThat(records).hasSize(75);
        assertThat(records).extracting(record -> ((GenericRecord) record).get("name").toString())
                .doesNotContain("name1")
                .doesNotContain("name2")
                .doesNotContain("name24")
                .contains("name25")
                .contains("name26")
                .contains("name99");
    }

    @Test
    void testGetRecordsWithInvalidData() {
        final byte[] invalidData = "invalid data".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(invalidData);
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> inputStream;

        final SourceCommonConfig s3SourceConfig = mock(SourceCommonConfig.class);

        final String topic = "test-topic";
        final int topicPartition = 0;

        final Stream<GenericRecord> records = parquetTransformer.getRecords(inputStreamIOSupplier, topic,
                topicPartition, s3SourceConfig, 0L);
        assertThat(records).isEmpty();
    }

    @Test
    void testTemporaryFileDeletion() throws Exception {
        final Path tempFile = Files.createTempFile("test-file", ".parquet");
        assertThat(Files.exists(tempFile)).isTrue();

        ParquetTransformer.deleteTmpFile(tempFile);
        assertThat(Files.exists(tempFile)).isFalse();
    }

    static byte[] generateMockParquetData() throws IOException {
        final Path path = ContentUtils.getTmpFilePath("name");
        return IOUtils.toByteArray(Files.newInputStream(path));
    }

    @Test
    void testIOExceptionCreatingTempFile() {
        try (var mockStatic = Mockito.mockStatic(File.class)) {
            mockStatic.when(() -> File.createTempFile(anyString(), anyString()))
                    .thenThrow(new IOException("Test IOException for temp file"));

            final IOSupplier<InputStream> inputStreamSupplier = mock(IOSupplier.class);
            final Stream<GenericRecord> resultStream = parquetTransformer.getRecords(inputStreamSupplier, "test-topic",
                    1, null, 0L);

            assertThat(resultStream).isEmpty();
        }
    }

    @Test
    void testIOExceptionDuringDataCopy() throws IOException {
        try (InputStream inputStreamMock = mock(InputStream.class)) {
            when(inputStreamMock.read(any(byte[].class))).thenThrow(new IOException("Test IOException during copy"));

            final IOSupplier<InputStream> inputStreamSupplier = () -> inputStreamMock;
            final Stream<GenericRecord> resultStream = parquetTransformer.getRecords(inputStreamSupplier, "test-topic",
                    1, null, 0L);

            assertThat(resultStream).isEmpty();
        }
    }
}
