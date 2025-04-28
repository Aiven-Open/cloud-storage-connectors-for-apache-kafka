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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A testing fixture to generate Avro test data.
 */
public final class AvroTestDataFixture {

    /** The Json string used to create the {@link #DEFAULT_SCHEMA} */
    public static final String SCHEMA_JSON = "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n  ]\n}";


    final String schemaFmt1 = "{\"id\" : \"%1$s\", \"value\" : \"value%1$s\"}%n";
    final String schemaFmt2 = "{\"id\" : \"%s\", \"message\" : \"from partition %s epoch %s\", \"value\" : \"value%s\"}%n";

//    //    private final Schema avroInputDataSchema = new Schema.Parser().parse(
////            "{\"type\":\"record\",\"name\":\"input_data\"," + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");
//
//    // Connect will add two extra fields to schema and enrich it with
//    // connect.version: 1
//    // connect.name: input_data
//    final Schema avroInputDataSchemaWithConnectExtra = new Schema.Parser()
//            .parse("{\"type\":\"record\",\"name\":\"input_data\","
//                    + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}],"
//                    + "\"connect.version\":1,\"connect.name\":\"input_data\"}");

    public static final String CONNECT_EXTRA_SCHEMA_JSON =  "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"}\n  ],\n"
            + "    \"connect.version\":1, \"connect.name\": \"TestRecord\"}\n";

//    final Schema evolvedAvroInputDataSchema = new Schema.Parser()
//            .parse("{\"type\":\"record\",\"name\":\"input_data\","
//                    + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\",\"default\":0}]}");

    public static final String EVOLVED_SCHEMA_JSON =  "{\n  \"type\": \"record\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int\"},\n"
            + "    {\"name\": \"age\", \"type\": \"int\", \"default\":0}\n  ]\n}";


    /** The schema used for most testing. Created from {@link #SCHEMA_JSON}. */
    public static final Schema DEFAULT_SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public static final Schema CONNECT_EXTRA_SCHEMA = new Schema.Parser().parse(CONNECT_EXTRA_SCHEMA_JSON);

    public static final Schema EVOLVED_SCHEMA = new Schema.Parser().parse(EVOLVED_SCHEMA_JSON);

    private AvroTestDataFixture() {
        // do not instantiate
    }

    public static String formatDefaultData(final int id, final String message) {
        return String.format("{\"id\" : \"%1$s\", \"message\" : \"%2$s\"}%n", id, message);
    }

    public static String formatEvolvedData(final int id, final String message, int age) {
        return String.format("{\"id\" : \"%1$s\", \"message\" : \"%2$s\", \"age\" : \"%3$s\"}%n", id, message, age);
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

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static List<GenericRecord> produceRecords(final KafkaProducer<String, GenericRecord> producer, final int recordCountPerPartition, final int partitionCount,
                                                      final String topicName) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount);
        int cnt = 0;
        for (final GenericRecord value : genericRecords) {
            final int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key, value)));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static List<GenericRecord> produceRecords(final KafkaProducer<String, GenericRecord> producer, final int recordCountPerPartition, final int partitionCount,
                                                     final String topicName, Function<Integer, GenericRecord> recordCreator) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount, recordCreator);
        int cnt = 0;
        for (final GenericRecord value : genericRecords) {
            final int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key, value)));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
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
    public static byte[] generateAvroData(final int messageId, final int numOfRecs,
            Function<Integer, GenericRecord> recordCreator) throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = generateAvroRecords(messageId, numOfRecs, recordCreator);

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
     * @return A byte array containing the specified number of records.
     */
    public static List<GenericRecord> generateAvroRecords(final int numRecs,
            Function<Integer, GenericRecord> recordCreator) {
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
            Function<Integer, GenericRecord> recordCreator) {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        final int limit = messageId + numOfRecs;
        for (int i = messageId; i < limit; i++) {
            avroRecords.add(recordCreator.apply(i));
        }
        return avroRecords;
    }

    public static GenericRecord generateAvroRecord(final int messageId) {
        return generateAvroRecord(messageId, DEFAULT_SCHEMA);
    }

    public static GenericRecord generateAvroRecord(final int messageId, Schema schema) {
        final GenericRecord avroRecord = new GenericData.Record(schema);
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
}
