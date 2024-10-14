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

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

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
        final String schemaString = "{" + "\"type\":\"record\"," + "\"name\":\"User\"," + "\"fields\":["
                + "{\"name\":\"name\",\"type\":\"string\"}," + "{\"name\":\"age\",\"type\":\"int\"},"
                + "{\"name\":\"email\",\"type\":\"string\"}" + "]" + "}";
        final Schema schema = new Schema.Parser().parse(schemaString);

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

        // Create sample records
        GenericData.Record user;

        // Create a Parquet writer
        final OutputFile outputFile = new LocalOutputFile(Paths.get(outputPath));
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(100L * 1024L)
                .withPageSize(1024 * 1024)
                .build()) {
            // Write records to the Parquet file
            for (int i = 0; i < numOfRecords; i++) {
                user = new GenericData.Record(schema);
                user.put("name", name1 + i);
                user.put("age", 30);
                user.put("email", name1 + "@test");

                writer.write(user);
            }

        }
    }
}
