package io.aiven.kafka.connect.common.config;

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


public class ParquetTestingFixture {

    public static Path getTmpFilePath(final String name1) throws IOException {
        final String tmpFile = "users.parquet";
        final Path parquetFileDir = Files.createTempDirectory("parquet_tests");
        final String parquetFilePath = parquetFileDir.toAbsolutePath() + "/" + tmpFile;

        writeParquetFile(parquetFilePath, name1);
        return Paths.get(parquetFilePath);
    }

    /**
     * Gets the schema used for the test cases.
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
     * Writes 100 parquet files to the file specified using the default schema.
     * @param outputFilePath
     * @param name
     * @throws IOException
     */
    public static void writeParquetFile(final Path outputFilePath, final String name) throws IOException {
            writeParquetFile(outputFilePath, name, 100);
    }

    public static void writeParquetFile(final Path outputFilePath, final String name, final int numberOfRecords) throws IOException {}

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static void writeParquetFile(final Path outputFilePath, final String name,
                                         final int numOfRecords) throws IOException {

        Schema schema = testSchema();
        final List<Struct> allParquetRecords = new ArrayList<>();
        // Write records to the Parquet file
        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords
                    .add(new Struct(schema).put("name", name + i).put("age", 30).put("email", name + "@test"));
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

    }
}
