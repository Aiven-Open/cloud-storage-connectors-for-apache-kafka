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

package io.aiven.kafka.connect.common.source.input;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.tuple.Pair;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A testing fixture to generate Avro test data.
 */
public final class AvroTestDataFixture {

    /** The Json string used to create the {@link #DEFAULT_SCHEMA} */
    public static final String SCHEMA_JSON = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";

    /** The schema used for most testing. Created from {@link #SCHEMA_JSON}. */
    public static final Schema DEFAULT_SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private AvroTestDataFixture() {
        // do not instantiate
    }

    /**
     * Generates a byte array containing the specified number of records.
     *
     * @param messageId
     *            the messageId to start with.
     * @param numRecs
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be generated.
     */
    public static byte[] generateAvroData(final int messageId, final int numRecs) throws IOException {
        return generateAvroData(messageId, DEFAULT_SCHEMA, numRecs);
    }

    /**
     * Generates a byte array containing the specified number of records.
     *
     * @param numRecs
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be serialized.
     */
    public static byte[] generateAvroData(final int numRecs) throws IOException {
        return generateAvroData(0, DEFAULT_SCHEMA, numRecs);
    }

    /**
     * creates and serializes the specified number of records with the specified schema.
     *
     * @param messageId
     *            the messageId to start with.
     * @param schema
     *            the schema to serialize with.
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be serialized.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] generateAvroData(final int messageId, final Schema schema, final int numOfRecs)
            throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = generateAvroRecords(messageId, schema, numOfRecs);

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

    /**
     * Creates the specified number of records with the default schema.
     *
     * @param numRecs
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     */
    public static List<GenericRecord> generateAvroRecords(final int numRecs) {
        return generateAvroRecords(0, DEFAULT_SCHEMA, numRecs);
    }

    /**
     * Creates the specified number of records with the specified schema.
     *
     * @param messageId
     *            the messageId to start with.
     * @param schema
     *            the schema to serialize with.
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static List<GenericRecord> generateAvroRecords(final int messageId, final Schema schema, final int numOfRecs) {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        final int limit = messageId + numOfRecs;
        for (int i = messageId; i < limit; i++) {
            avroRecords.add(generateAvroRecord(i, schema));
        }
        return avroRecords;
    }

    public static GenericRecord generateAvroRecord(final int messageId) {
        return generateAvroRecord(messageId, DEFAULT_SCHEMA);
    }

    public static GenericRecord generateAvroRecord(final int messageId, final Schema schema) {
        final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD AvoidInstantiatingObjectsInLoops
        avroRecord.put("message", "Hello, from Avro Test Data Fixture! object " + messageId);
        avroRecord.put("id", messageId);
        return avroRecord;
    }

    public static List<GenericRecord> readAvroRecords(byte[] bytes) throws IOException {
        List<GenericRecord> result = new ArrayList<>();
        try (SeekableInput sin = new SeekableByteArrayInput(bytes)) {
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                reader.forEach(result::add);
            }
        }
        return result;
    }

    public static Pair<String, GenericRecord> parseGenericRecord(GenericRecord avroRecord) throws IOException {
        return Pair.of(avroRecord.get("key").toString(),
                (GenericRecord) avroRecord.get("value"));
    }
}
