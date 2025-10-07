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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

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

/**
 * A testing fixture to generate/read Avro test data.
 */
public final class AvroTestDataFixture {

    /** The Json string used to create the {@link #DEFAULT_SCHEMA} */
    public static final String SCHEMA_JSON = "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n  ]\n}";

    /** The Json string used to create the {@link #CONNECT_EXTRA_SCHEMA} */
    public static final String CONNECT_EXTRA_SCHEMA_JSON = "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n  ],\n"
            + "    \"connect.version\":1, \"connect.name\": \"TestRecord\"}\n";

    /** The Json string used to create the {@link #EVOLVED_SCHEMA} */
    public static final String EVOLVED_SCHEMA_JSON = "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"},\n"
            + "    {\"name\": \"age\", \"type\": \"int\", \"default\":0}\n  ]\n}";

    /** The schema used for most testing. Created from {@link #SCHEMA_JSON}. */
    public static final Schema DEFAULT_SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    /** The schema used when testing plain data without envelope. Created from {@link #CONNECT_EXTRA_SCHEMA_JSON}. */
    public static final Schema CONNECT_EXTRA_SCHEMA = new Schema.Parser().parse(CONNECT_EXTRA_SCHEMA_JSON);

    /** The schema used to test the evolution of schema. Created from {@link #EVOLVED_SCHEMA_JSON}. */
    public static final Schema EVOLVED_SCHEMA = new Schema.Parser().parse(EVOLVED_SCHEMA_JSON);

    private AvroTestDataFixture() {
        // do not instantiate
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
        return generateAvroData(0, numRecs);
    }

    /**
     * creates and serializes the specified number of records with the specified schema.
     *
     * @param messageId
     *            the messageId to start with.
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be serialized.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] generateAvroData(final int messageId, final int numOfRecs) throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = generateAvroRecords(messageId, numOfRecs);

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(DEFAULT_SCHEMA);

        // Append each record using a loop
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(DEFAULT_SCHEMA, outputStream);
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
        return generateAvroRecords(0, numRecs);
    }

    /**
     * Creates the specified number of records with the default schema.
     *
     * @param numRecs
     *            the numer of records to generate
     * @param recordCreator A function to convert the record number into a generic record.
     * @return A byte array containing the specified number of records.
     */
    public static List<GenericRecord> generateAvroRecords(final int numRecs,
            final Function<Integer, GenericRecord> recordCreator) {
        return generateAvroRecords(0, numRecs, recordCreator);
    }

    /**
     * Creates the specified number of records with the specified schema.
     *
     * @param messageId
     *            the messageId to start with.
     *
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static List<GenericRecord> generateAvroRecords(final int messageId, final int numOfRecs) {
        return generateAvroRecords(messageId, numOfRecs, AvroTestDataFixture::generateAvroRecord);
    }

    /**
     * Creates the specified number of records with the specified schema.
     *
     * @param messageId
     *            the messageId to start with.
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static List<GenericRecord> generateAvroRecords(final int messageId, final int numOfRecs,
            final Function<Integer, GenericRecord> recordCreator) {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        final int limit = messageId + numOfRecs;
        for (int i = messageId; i < limit; i++) {
            avroRecords.add(recordCreator.apply(i));
        }
        return avroRecords;
    }

    /**
     * Generate an avro record with the specified message id using the default schema.
     *
     * @param messageId
     *            the message id.
     * @return a GenericRecord with the specified data.
     */
    public static GenericRecord generateAvroRecord(final int messageId) {
        return generateAvroRecord(messageId, DEFAULT_SCHEMA);
    }

    /**
     * Generate an avro record with the specified message id using the specified schema
     *
     * @param messageId
     *            the message id.
     * @param schema
     *            the schaema to use.
     * @return a GenericRecord with the specified data and schema.
     */
    public static GenericRecord generateAvroRecord(final int messageId, final Schema schema) {
        final GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("message", "Hello, from Avro Test Data Fixture! object " + messageId);
        avroRecord.put("id", messageId);
        if (schema.getField("age") != null) {
            avroRecord.put("age", messageId);
        }
        return avroRecord;
    }

    /**
     * Extracts Avro records from a byte array.
     *
     * @param bytes
     *            the byte array to extract the records from.
     * @return the GenericRecords from the byte array.
     * @throws IOException
     *             on read error.
     */
    public static List<GenericRecord> readAvroRecords(final byte[] bytes) throws IOException {
        final List<GenericRecord> result = new ArrayList<>();
        try (SeekableInput sin = new SeekableByteArrayInput(bytes)) {
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                reader.forEach(result::add);
            }
        }
        return result;
    }
}
