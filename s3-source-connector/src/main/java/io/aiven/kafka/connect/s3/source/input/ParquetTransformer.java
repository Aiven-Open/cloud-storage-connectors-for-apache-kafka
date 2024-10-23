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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public List<Object> getRecords(final InputStream inputStream, final String topic, final int topicPartition,
            final S3SourceConfig s3SourceConfig) {
        return getParquetRecords(inputStream, topic, topicPartition);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final S3SourceConfig s3SourceConfig) {
        return TransformationUtils.serializeAvroRecordToBytes(Collections.singletonList((GenericRecord) record), topic,
                s3SourceConfig);
    }

    private List<Object> getParquetRecords(final InputStream inputStream, final String topic,
            final int topicPartition) {
        final String timestamp = String.valueOf(Instant.now().toEpochMilli());
        File parquetFile;
        final List<Object> records = new ArrayList<>();
        try {
            parquetFile = File.createTempFile(topic + "_" + topicPartition + "_" + timestamp, ".parquet");
        } catch (IOException e) {
            LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
            return records;
        }

        try (OutputStream outputStream = Files.newOutputStream(parquetFile.toPath())) {
            IOUtils.copy(inputStream, outputStream);
            final InputFile inputFile = new LocalInputFile(parquetFile.toPath());
            try (var parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                GenericRecord record;
                record = parquetReader.read();
                while (record != null) {
                    records.add(record);
                    record = parquetReader.read();
                }
            }
        } catch (IOException | RuntimeException e) { // NOPMD
            LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
        } finally {
            deleteTmpFile(parquetFile.toPath());
        }
        return records;
    }

    static void deleteTmpFile(final Path parquetFile) {
        if (Files.exists(parquetFile)) {
            try {
                Files.delete(parquetFile);
            } catch (IOException e) {
                LOGGER.error("Error in deleting tmp file {}", e.getMessage(), e);
            }
        }
    }
}
