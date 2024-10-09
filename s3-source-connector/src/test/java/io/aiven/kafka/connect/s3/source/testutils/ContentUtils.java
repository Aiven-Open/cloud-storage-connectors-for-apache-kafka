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
    public static Path getTmpFilePath(final String name1, final String name2) throws IOException {
        final String tmpFile = "users.parquet";
        final Path parquetFileDir = Files.createTempDirectory("parquet_tests");
        final String parquetFilePath = parquetFileDir.toAbsolutePath() + "/" + tmpFile;

        writeParquetFile(parquetFilePath, name1, name2);
        return Paths.get(parquetFilePath);
    }

    public static void writeParquetFile(final String tempFilePath, final String name1, final String name2)
            throws IOException {
        // Define the Avro schema
        final String schemaString = "{" + "\"type\":\"record\"," + "\"name\":\"User\"," + "\"fields\":["
                + "{\"name\":\"name\",\"type\":\"string\"}," + "{\"name\":\"age\",\"type\":\"int\"},"
                + "{\"name\":\"email\",\"type\":\"string\"}" + "]" + "}";
        final Schema schema = new Schema.Parser().parse(schemaString);

        // Write the Parquet file
        try {
            writeParquetFile(tempFilePath, schema, name1, name2);
        } catch (IOException e) {
            throw new ConnectException("Error writing parquet file");
        }
    }

    private static void writeParquetFile(final String outputPath, final Schema schema, final String name1,
            final String name2) throws IOException {

        // Create sample records
        final GenericData.Record user1 = new GenericData.Record(schema);
        user1.put("name", name1);
        user1.put("age", 30);
        user1.put("email", name1 + "@test");

        final GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name", name2);
        user2.put("age", 25);
        user2.put("email", name2 + "@test");

        // Create a Parquet writer
        final OutputFile outputFile = new LocalOutputFile(Paths.get(outputPath));
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(100L * 1024L)
                .withPageSize(1024 * 1024)
                .build()) {
            // Write records to the Parquet file
            writer.write(user1);
            writer.write(user2);
        }
    }
}
