package io.aiven.kafka.connect.common.source.input;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.ParquetTestingFixture;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

public class ParquetTestDataFixture {


    public static byte[] generateMockParquetData(final String name, final int numOfRecords)
            throws IOException {
        Schema schema = ParquetTestingFixture.testSchema();

        final List<Struct> allParquetRecords = new ArrayList<>();
        // Write records to the Parquet file
        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords.add(new Struct(schema).put("name", name + i).put("age", 30).put("email", name + "@test"));
        }

        // Create a Parquet writer
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (var parquetWriter = new ParquetOutputWriter(
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
        return outputStream.toByteArray();
    }
}
