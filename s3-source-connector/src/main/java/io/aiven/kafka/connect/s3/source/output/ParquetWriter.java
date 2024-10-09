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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWriter implements OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetWriter.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public void handleValueData(final Optional<byte[]> optionalKeyBytes, final InputStream inputStream,
            final String topic, final List<ConsumerRecord<byte[], byte[]>> consumerRecordList,
            final S3SourceConfig s3SourceConfig, final int topicPartition, final long startOffset,
            final OffsetManager offsetManager, final Map<Map<String, Object>, Long> currentOffsets,
            final Map<String, Object> partitionMap) {
        final List<GenericRecord> records = getRecords(inputStream, topic, topicPartition);
        OutputUtils.buildConsumerRecordList(optionalKeyBytes, topic, consumerRecordList, s3SourceConfig, topicPartition,
                startOffset, offsetManager, currentOffsets, records, partitionMap);
    }

    public static List<GenericRecord> getRecords(final InputStream inputStream, final String topic,
            final int topicPartition) {
        final String timestamp = String.valueOf(Instant.now().toEpochMilli());
        File parquetFile;
        final var records = new ArrayList<GenericRecord>();
        try {
            parquetFile = File.createTempFile(topic + "_" + topicPartition + "_" + timestamp, ".parquet");
        } catch (IOException e) {
            LOGGER.error("Error in reading s3 object stream " + e.getMessage());
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
            LOGGER.error("Error in reading s3 object stream " + e.getMessage());
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
                LOGGER.error("Error in deleting tmp file " + e.getMessage());
            }
        }
    }
}
