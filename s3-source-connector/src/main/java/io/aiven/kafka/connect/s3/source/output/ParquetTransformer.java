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
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the transform from the S3 input stream Parquet based data for the @{code byte[]} found in a {@code ConsumerRecord<byte[], byte[]>}.
 * Each Avro record in the Parquet file will be converted to a {@code byte[]}.
 */
public class ParquetTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTransformer.class);

    @Override
    public String getName() {
        return "parquet";
    }

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public Iterator<byte[]> byteArrayIterator(InputStream inputStream, String topic, S3SourceConfig s3SourceConfig) throws BadDataException {
        try {
            List<byte[]> result = new ArrayList<>();
            KafkaAvroSerializer avroSerializer = AvroTransformer.createAvroSerializer(s3SourceConfig);
            for (GenericRecord genericRecord : getParquetRecords(inputStream, topic)) {
                result.add(avroSerializer.serialize(topic, genericRecord));
            }
            return result.iterator();
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new BadDataException(e);
        }
    }

    /**
     * Reads the parquet records from the input stream.
     * @param inputStream The input stream to read.
     * @param topic the Kafka topic the data will be placed on.
     * @return a list of GenericRecords extracted from the stream.
     * @throws BadDataException if the data can not be read as a Parquet structure.
     */
    private List<GenericRecord> getParquetRecords(final InputStream inputStream, final String topic) throws BadDataException {
        File parquetFile = null;
        try {
            parquetFile = File.createTempFile(topic + "_", ".parquet");
            try (OutputStream outputStream = Files.newOutputStream(parquetFile.toPath())) {
                IOUtils.copy(inputStream, outputStream);
                final InputFile inputFile = new LocalInputFile(parquetFile.toPath());
                try (var parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                    final List<GenericRecord> records = new ArrayList<>();
                    GenericRecord record = parquetReader.read();
                    while (record != null) {
                        records.add(record);
                        record = parquetReader.read();
                    }
                    return records;
                }
            } catch (RuntimeException e) { // NOPMD
                throw new BadDataException(e);
            }
        } catch (IOException e) {
            throw new BadDataException(e);
        } finally {
            if (parquetFile != null) {
                deleteTmpFile(parquetFile.toPath());
            }
        }
    }

    /**
     * Deletes a temporary file if it exists and logs any exceptions hit in the process.
     * @param tempFile the file to delete.
     */
    // package private for testing.
    static void deleteTmpFile(final Path tempFile) {
        if (Files.exists(tempFile)) {
            try {
                Files.delete(tempFile);
            } catch (IOException e) {
                LOGGER.error("Error in deleting tmp file " + e.getMessage());
            }
        }
    }
}
