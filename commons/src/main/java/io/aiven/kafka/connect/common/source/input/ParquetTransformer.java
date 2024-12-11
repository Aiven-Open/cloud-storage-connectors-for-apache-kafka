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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMA_REGISTRY_URL;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;

import io.aiven.kafka.connect.common.source.input.parquet.LocalInputFile;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, sourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public Stream<Object> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig, final long skipRecords) {
        return getParquetStreamRecords(inputStreamIOSupplier, topic, topicPartition, skipRecords);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final AbstractConfig sourceConfig) {
        return TransformationUtils.serializeAvroRecordToBytes(Collections.singletonList((GenericRecord) record), topic,
                sourceConfig);
    }

    private Stream<Object> getParquetStreamRecords(final IOSupplier<InputStream> inputStreamIOSupplier,
            final String topic, final int topicPartition, final long skipRecords) {
        final String timestamp = String.valueOf(Instant.now().toEpochMilli());
        File parquetFile;

        try {
            // Create a temporary file for the Parquet data
            parquetFile = File.createTempFile(topic + "_" + topicPartition + "_" + timestamp, ".parquet");
        } catch (IOException e) {
            LOGGER.error("Error creating temp file for Parquet data: {}", e.getMessage(), e);
            return Stream.empty();
        }

        try (OutputStream outputStream = Files.newOutputStream(parquetFile.toPath());
                InputStream inputStream = inputStreamIOSupplier.get();) {
            IOUtils.copy(inputStream, outputStream); // Copy input stream to temporary file

            final InputFile inputFile = new LocalInputFile(parquetFile.toPath());
            final var parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build();

            return StreamSupport.stream(new Spliterators.AbstractSpliterator<Object>(Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.NONNULL) {
                @Override
                public boolean tryAdvance(final java.util.function.Consumer<? super Object> action) {
                    try {
                        final GenericRecord record = parquetReader.read();
                        if (record != null) {
                            action.accept(record); // Pass record to the stream
                            return true;
                        } else {
                            parquetReader.close(); // Close reader at end of file
                            deleteTmpFile(parquetFile.toPath());
                            return false;
                        }
                    } catch (IOException | RuntimeException e) { // NOPMD
                        LOGGER.error("Error reading Parquet record: {}", e.getMessage(), e);
                        deleteTmpFile(parquetFile.toPath());
                        return false;
                    }
                }
            }, false).skip(skipRecords).onClose(() -> {
                try {
                    parquetReader.close(); // Ensure reader is closed when the stream is closed
                } catch (IOException e) {
                    LOGGER.error("Error closing Parquet reader: {}", e.getMessage(), e);
                }
                deleteTmpFile(parquetFile.toPath());
            });
        } catch (IOException | RuntimeException e) { // NOPMD
            LOGGER.error("Error processing Parquet data: {}", e.getMessage(), e);
            deleteTmpFile(parquetFile.toPath());
            return Stream.empty();
        }
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
