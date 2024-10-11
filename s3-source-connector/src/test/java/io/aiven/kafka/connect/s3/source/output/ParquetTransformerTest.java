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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.ContentUtils;

import com.amazonaws.util.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ParquetTransformerTest {
    private ParquetTransformer underTest;

    private S3SourceConfig s3SourceConfig;

    @BeforeEach
    public void setUp() {
        underTest = new ParquetTransformer();
        s3SourceConfig = new S3SourceConfig(AvroTransformerTest.getBasicProperties());
    }

    @Test
    public void testName() {
        assertThat(underTest.getName().equals("parquet"));
    }

    @Test
    void testConfigureValueConverter() {
        final String value = "http://localhost:8081";
        Map<String, String> config = new HashMap<>();

        // test value
        underTest.configureValueConverter(config, s3SourceConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo("http://localhost:8081")
                .describedAs("The schema registry URL should be correctly set in the config.");

    }

    @Test
    void testHandleValueDataWithZeroBytes() {
        final InputStream inputStream = new ByteArrayInputStream(new byte[0]);

        final String topic = "test-topic";

        assertThatThrownBy(() -> underTest.byteArrayIterator(inputStream, "topic", s3SourceConfig))
                .isInstanceOf(BadDataException.class);
    }

    @Test
    void testGetRecordsWithValidData() throws Exception {
        final byte[] mockParquetData = generateMockParquetData();
        final InputStream inputStream = new ByteArrayInputStream(mockParquetData);

        final String topic = "test-topic";

        final Iterator<byte[]> iter = underTest.byteArrayIterator(inputStream, topic, s3SourceConfig);

        assertThat(iter).hasNext();
        byte[] actual = iter.next();
        System.out.println(actual);
    }

    @Test
    void testGetRecordsWithInvalidData() {
        final byte[] invalidData = "invalid data".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(invalidData);

        final String topic = "test-topic";
        assertThatThrownBy(() -> underTest.byteArrayIterator(inputStream, "topic", s3SourceConfig))
                .isInstanceOf(BadDataException.class);
    }

    @Test
    void testTemporaryFileDeletion() throws Exception {
        final Path tempFile = Files.createTempFile("test-file", ".parquet");
        assertThat(Files.exists(tempFile)).isTrue();

        ParquetTransformer.deleteTmpFile(tempFile);
        assertThat(Files.exists(tempFile)).isFalse();
    }

    private byte[] generateMockParquetData() throws IOException {
        final Path path = ContentUtils.getTmpFilePath("name1", "name2");
        return IOUtils.toByteArray(Files.newInputStream(path));
    }
}
