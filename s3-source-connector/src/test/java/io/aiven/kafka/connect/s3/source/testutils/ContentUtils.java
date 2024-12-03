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

package io.aiven.kafka.connect.s3.source.testutils;

import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;

public final class ContentUtils {
    private ContentUtils() {
    }
    public static Path getTmpFilePath(final String name1) throws IOException {
        final String tmpFile = "users.parquet";
        final Path parquetFileDir = Files.createTempDirectory("parquet_tests");
        final String parquetFilePath = parquetFileDir.toAbsolutePath() + "/" + tmpFile;

        writeParquetFile(parquetFilePath, name1);
        return Paths.get(parquetFilePath);
    }

    public static void writeParquetFile(final String tempFilePath, final String name1) throws IOException {
        // Define the Avro schema
        final Schema schema = SchemaBuilder.struct()
                .field("name", STRING_SCHEMA)
                .field("age", INT32_SCHEMA)
                .field("email", STRING_SCHEMA)
                .build();
        // Write the Parquet file
        try {
            writeParquetFile(tempFilePath, schema, name1, 100);
        } catch (IOException e) {
            throw new ConnectException("Error writing parquet file");
        }
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static void writeParquetFile(final String outputPath, final Schema schema, final String name1,
            final int numOfRecords) throws IOException {

        final List<Struct> allParquetRecords = new ArrayList<>();

        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords
                    .add(new Struct(schema).put("name", name1 + i).put("age", 30).put("email", name1 + "@test"));
        }

        // Create a Parquet writer
        final Path outputFilePath = Paths.get(outputPath);
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

    }
}
