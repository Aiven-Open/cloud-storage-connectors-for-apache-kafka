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

package io.aiven.kafka.connect.common.config;

import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;

/**
 * Test fixture to generate standard parquet file.
 */
public class ParquetTestingFixture {
    /**
     * Gets the schema used for the test cases.
     *
     * @return The schema used for the test cases.
     */
    public static Schema testSchema() {
        return SchemaBuilder.struct()
                .field("name", STRING_SCHEMA)
                .field("age", INT32_SCHEMA)
                .field("email", STRING_SCHEMA)
                .build();
    }
    /**
     * Writes 100 parquet records to the file specified using the default schema. The topic "some-topic" will be used
     * for each record.
     * "some-key-#" will be used for each key.
     *
     * @param outputFilePath
     *            the path the to the output file.
     * @param name
     *            the name used for each record. The record number will be appended to the name.
     * @throws IOException
     *             on output error.
     */
    public static Path writeParquetFile(final Path outputFilePath, final String name) throws IOException {
        return writeParquetFile(outputFilePath, name, 100);
    }

    /**
     * Writes the specified number of parquet records to the file specified using the default schema. The topic
     * "some-topic" will be used for each record.
     * "some-key-#" will be used for each key.
     *
     * @param outputFilePath
     *            the path the to the output file.
     * @param name
     *            the name used for each record. The record number will be appended to the name.
     * @param numOfRecords
     *            the number of records to write.
     * @throws IOException
     *             on output error.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static Path writeParquetFile(final Path outputFilePath, final String name, final int numOfRecords)
            throws IOException {
        Schema schema;
        schema = testSchema();
        final List<Struct> allParquetRecords = new ArrayList<>();
        // Write records to the Parquet file
        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords.add(new Struct(schema).put("name", name + i).put("age", 30).put("email", name + "@test"));
        }

        // Create a Parquet writer
        try (var outputStream = Files.newOutputStream(outputFilePath.toAbsolutePath());
                var parquetWriter = new ParquetOutputWriter(
                        List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)), outputStream,
                        Collections.emptyMap(), false)) {
            int counter = 0;
            final var sinkRecords = new ArrayList<SinkRecord>();
            for (final var r : allParquetRecords) {
                final var sinkRecord = new SinkRecord( // NOPMD AvoidInstantiatingObjectsInLoops
                        "some-topic", 1, STRING_SCHEMA, "some-key-" + counter, schema, r, 100L, 1000L + counter,
                        TimestampType.CREATE_TIME, null);
                sinkRecords.add(sinkRecord);
                counter++;
            }
            parquetWriter.writeRecords(sinkRecords);
        }
        return outputFilePath;
    }
}
