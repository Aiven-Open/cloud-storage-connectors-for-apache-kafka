package io.aiven.kafka.connect.common.source.input;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public final class AvroTestDataFixture {
    public static byte[] generateMockAvroData(final int numRecs) throws IOException {
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        return getAvroRecords(schema, numRecs);
    }

    private static byte[] getAvroRecords(final Schema schema, final int numOfRecs) throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        for (int i = 0; i < numOfRecs; i++) {
            final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD AvoidInstantiatingObjectsInLoops
            avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + i);
            avroRecord.put("id", i);
            avroRecords.add(avroRecord);
        }

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        // Append each record using a loop
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            for (final GenericRecord record : avroRecords) {
                dataFileWriter.append(record);
            }
            dataFileWriter.flush();
        }
        outputStream.close();
        return outputStream.toByteArray();
    }
}
