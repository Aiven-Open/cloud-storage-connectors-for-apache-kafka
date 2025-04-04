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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A testing fixture to generate Avro test data.
 */
public final class AvroTestDataFixture {

    /** The Json string used to create the {@link #DEFAULT_SCHEMA} */
    public static String SCHEMA_JSON = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";

    /** The schema used for most testing.  Created from {@link #SCHEMA_JSON}. */
    public static Schema DEFAULT_SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    /**
     * Generates a byte array containing the specified number of records.
     *
     * @param messageId the messageId to start with.
     * @param numRecs
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be generated.
     */
    public static byte[] generateMockAvroData(final int messageId, final int numRecs) throws IOException {
        return generateMockAvroData(messageId, DEFAULT_SCHEMA, numRecs);
    }

    /**
     * Generates a byte array containing the specified number of records.
     *
     * @param numRecs
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be generated.
     */
    public static byte[] generateMockAvroData(final int numRecs) throws IOException {
        return generateMockAvroData(0, DEFAULT_SCHEMA, numRecs);
    }

    /**
     * creates and serializes the specified number of records with the specified schema.
     *
     * @param messageId the messageId to start with.
     * @param schema
     *            the schema to serialize with.
     * @param numOfRecs
     *            the number of records to write.
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be generated.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] generateMockAvroData(final int messageId, final Schema schema, final int numOfRecs) throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        int limit = messageId + numOfRecs;
        for (int i = messageId; i < limit; i++) {
            final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD AvoidInstantiatingObjectsInLoops
            avroRecord.put("message", "Hello, Kafka Connect Abstract Source! object " + i);
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
