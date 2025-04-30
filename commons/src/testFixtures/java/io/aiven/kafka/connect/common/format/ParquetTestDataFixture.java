/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.common.format;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.ParquetTestingFixture;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

/**
 * A testing feature to generate/read Parquet data.
 */
public final class ParquetTestDataFixture {

    public static final Schema NAME_VALUE_SCHEMA = SchemaBuilder.record("value")
            .fields()
            .name("name")
            .type()
            .stringType()
            .noDefault()
            .name("value")
            .type()
            .stringType()
            .noDefault()
            .endRecord();

    public static final Schema EVOLVED_NAME_VALUE_SCHEMA = SchemaBuilder.record("value")
            .fields()
            .name("name")
            .type()
            .stringType()
            .noDefault()
            .name("value")
            .type()
            .stringType()
            .noDefault()
            .name("blocked")
            .type()
            .booleanType()
            .booleanDefault(false)
            .endRecord();

    private ParquetTestDataFixture() {
        // do not instantiate
    }
    /**
     * Generate the specified number of parquet records in a byte array.
     *
     * @param name
     *            the name to be used in each object. Each object will have a "name" property with the value of
     *            {@code name} followed by the record number.
     * @param numOfRecords
     *            The number of records to generate.
     * @return A byte array containing the specified number of parquet records.
     * @throws IOException
     *             if the data can not be written.
     */
    @SuppressWarnings({ "PMD.DataflowAnomalyAnalysis", "PMD.AvoidInstantiatingObjectsInLoops" })
    public static byte[] generateParquetData(final String name, final int numOfRecords) throws IOException {

        final List<Struct> allParquetRecords = new ArrayList<>();
        // Write records to the Parquet file
        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords.add(new Struct(ParquetTestingFixture.PARQUET_SCHEMA).put("name", name + i)
                    .put("age", 30)
                    .put("email", name + "@test"));
        }

        // Create a Parquet writer
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ParquetOutputWriter parquetWriter = new ParquetOutputWriter(
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)), outputStream,
                Collections.emptyMap(), false)) {
            int counter = 0;
            final var sinkRecords = new ArrayList<SinkRecord>();
            for (final var r : allParquetRecords) {
                final var sinkRecord = new SinkRecord("some-topic", 1, STRING_SCHEMA, "some-key-" + counter,
                        ParquetTestingFixture.PARQUET_SCHEMA, r, 100L, 1000L + counter, TimestampType.CREATE_TIME,
                        null);
                sinkRecords.add(sinkRecord);
                counter++;
            }
            parquetWriter.writeRecords(sinkRecords);
        }
        return outputStream.toByteArray();
    }

    /**
     * Reads records.
     * @param tmpDir the temporary directory to write files to.
     * @param bytes the bytes to write to a file.
     * @return the List of GenericRecords extracted from the bytes data.
     * @throws IOException on IO error.
     */
    public static List<GenericRecord> readRecords(final Path tmpDir, final byte[] bytes) throws IOException {
        final var records = new ArrayList<GenericRecord>();
        final var parquetFile = tmpDir.resolve("parquet.file");
        FileUtils.writeByteArrayToFile(parquetFile.toFile(), bytes);
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(parquetFile);
                var parquetReader = AvroParquetReader.<GenericRecord>builder(new InputFile() {
                    @Override
                    public long getLength() throws IOException {
                        return seekableByteChannel.size();
                    }

                    @Override
                    public SeekableInputStream newStream() throws IOException {
                        return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {
                            @Override
                            public long getPos() throws IOException {
                                return seekableByteChannel.position();
                            }

                            @Override
                            public void seek(final long position) throws IOException {
                                seekableByteChannel.position(position);
                            }
                        };
                    }

                }).withCompatibility(false).build()) {
            var record = parquetReader.read();
            while (record != null) {
                records.add(record);
                record = parquetReader.read();
            }
        }
        return records;
    }
}
