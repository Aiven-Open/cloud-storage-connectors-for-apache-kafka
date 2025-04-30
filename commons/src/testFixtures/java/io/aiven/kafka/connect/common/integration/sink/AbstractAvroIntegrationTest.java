/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.integration.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The collecton of defined tests for Avro Sink.
 * @param <N> the native storage object type
 * @param <K> the native storage key type.
 */
public abstract class AbstractAvroIntegrationTest<N, K extends Comparable<K>>
        extends
            AbstractSinkIntegrationTest<N, K> {
    /** The @{code null} value used ot clear the KafkaProducer when it is no longer needed */
    private static final KafkaProducer<String, GenericRecord> NULL_PRODUCER = null;
    /** The KafkaProducer that this test uses */
    private KafkaProducer<String, GenericRecord> producer;

    /**
     * Clears the producer and removes the native storage.
     */
    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
        sinkStorage.removeStorage();
    }

    /**
     * Creates a configuration with the storage and general Avro configuration options.
     *
     * @param topics
     *            the topics to listend to.
     * @return ta configuration map.
     */
    @Override
    protected Map<String, String> createConfiguration(final String... topics) {
        final Map<String, String> config = super.createConfiguration(topics);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        return config;
    }

    /**
     * Creates the Avro codec / compression pairs for testing.
     * <p>
     *     Note: some combinations do not work.  These should be fixed or noted in the configuration documentation..
     * </p>
     * @return A stream of avro codec / compression type pairs.
     */
    private static Stream<Arguments> compressionAndCodecTestParameters() {
        final List<Arguments> lst = new ArrayList<>();

        /*
         // code that adds all combination of codec and compression.
         String[] codecs = {"null", "deflate", "snappy", "bzip2", "xz", "zstandard"};
         for (String codec : codecs) {
            for (CompressionType compression : CompressionType.values()) {
               lst.add(Arguments.of(codec, compression));
            }
         }
         */
        lst.add(Arguments.of("null", CompressionType.NONE));
        lst.add(Arguments.of("bzip2", CompressionType.NONE));
        lst.add(Arguments.of("deflate", CompressionType.NONE));
        lst.add(Arguments.of("snappy", CompressionType.GZIP));
        lst.add(Arguments.of("zstandard", CompressionType.NONE));

        return lst.stream();
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutput(final String avroCodec, final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic(avroCodec, compression.name);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE)
                .withFormatType(FormatType.AVRO);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);
        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K nativeKey : expectedBlobs) {
            for (final GenericRecord r : AvroTestDataFixture.readAvroRecords(readBytes(nativeKey, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(mapF("message")).collect(Collectors.toList());
        String[] expected = expectedGenericRecords.stream()
                .map(mapF("message"))
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("id")).collect(Collectors.toList());
        expected = expectedGenericRecords.stream().map(mapF("id")).collect(Collectors.toList()).toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    @Test
    void jsonlAvroOutputTest() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        /*
         * connectorConfig.put("format.output.fields", "key,value");
         * connectorConfig.put("format.output.fields.value.encoding", "none"); connectorConfig.put("key.converter",
         * "io.confluent.connect.avro.AvroConverter"); connectorConfig.put("value.converter",
         * "io.confluent.connect.avro.AvroConverter"); connectorConfig.put("value.converter.schemas.enable", "false");
         * connectorConfig.put("file.compression.type", compression.name()); connectorConfig.put("format.output.type",
         * contentType);
         */

        kafkaManager.configureConnector(connectorName, connectorConfig);
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();

        for (final K nativeKey : expectedBlobs) {
            for (final JsonNode node : JsonTestDataFixture.readJsonRecords(readBytes(nativeKey, compression))) {
                actualValues.add(node.findValue("message").asText());
            }
        }

        final String[] expected = expectedGenericRecords.stream().map(mapF("message")).toArray(String[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expected);
    }

    /**
     * Creates and sends a list of GenericRecords.  All sent records are acknowledged by the producer before this method
     * exits. Records are produced across the partitions.  For example the first record produced is assigned to partition 0,
     * the next to partition 1 and so on until all partitions have a record.  Then the second record is added.  This proceeds until
     * all partitions have the proper number of records.
     * @param recordCountPerPartition the number of records to put in each partition.
     * @param partitionCount the number of partitions.
     * @param topicName the topic name for the records.
     * @return the list of Generic records that were sent.
     * @throws ExecutionException if there is an issue generating or sending the records.
     * @throws InterruptedException if the process is interrupted.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<GenericRecord> produceRecords(final int recordCountPerPartition, final int partitionCount,
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

    /**
     * Creates and sends a list of GenericRecords using a custom format.  All sent records are acknowledged by the producer before this method
     * exits. Records are produced across the partitions.  For example the first record produced is assigned to partition 0,
     * the next to partition 1 and so on until all partitions have a record.  Then the second record is added.  This proceeds until
     * all partitions have the proper number of records.
     * @param recordCountPerPartition the number of records to put in each partition.
     * @param partitionCount the number of partitions.
     * @param topicName the topic name for the records.
     * @param recordGenerator the function to convert an integer into a GenericRecord.  See {@link AvroTestDataFixture#generateAvroRecord(int)} for an example.
     * @return the list of Generic records that were sent.
     * @throws ExecutionException if there is an issue generating or sending the records.
     * @throws InterruptedException if the process is interrupted.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<GenericRecord> produceRecords(final int recordCountPerPartition, final int partitionCount,
            final String topicName, final Function<Integer, GenericRecord> recordGenerator)
            throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount, recordGenerator);
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
     * Create a producer for this test suite.
     * @return the KafkaProducer for this test suite.
     */
    private KafkaProducer<String, GenericRecord> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }


    @Test
    void defaultAvroOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = sinkStorage.getDefaultCompression();
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K nativeKey : expectedBlobs) {
            for (final GenericRecord r : AvroTestDataFixture.readAvroRecords(readBytes(nativeKey, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(messageMapper.apply(value));
            }
        }

        final List<String> expected = expectedGenericRecords.stream().map(mapF("message")).collect(Collectors.toList());

        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expected);

    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutputPlainValueWithoutEnvelope(final String avroCodec, final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {

        final String topicName = getTopic(avroCodec, compression.name);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.VALUE)
                .envelopeEnabled(false);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        produceRecords(recordCountPerPartition, partitionCount, topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final List<GenericRecord> blobContents = new ArrayList<>();
        for (final K blobName : expectedBlobs) {
            blobContents.addAll(AvroTestDataFixture.readAvroRecords(readBytes(blobName, compression)));
        }

        final List<GenericRecord> enrichedValues = new ArrayList<>();
        enrichedValues.addAll(AvroTestDataFixture.generateAvroRecords(recordCountPerPartition * partitionCount,
                i -> AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.CONNECT_EXTRA_SCHEMA)));

        assertThat(enrichedValues).containsExactlyInAnyOrder(blobContents.toArray(GenericRecord[]::new));
    }

    /**
     * When Avro schema changes a new Avro Container File must be produced to Azure. Avro Container File can have only
     * records written with same schema.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {

        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();
        final CompressionType compression = sinkStorage.getDefaultCompression();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.VALUE)
                .envelopeEnabled(false)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        // Send only three records, assert three files created.
        final List<GenericRecord> expectedRecords = produceRecords(3, 1, topicName, i -> {
            if (i % 2 == 0) {
                final GenericRecord rec = AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.EVOLVED_SCHEMA);
                rec.put("age", i);
                return rec;
            }
            return AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.DEFAULT_SCHEMA);
        });

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 0, 1, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 0, 2, compression));

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final var actualValues = new ArrayList<String>();
        for (final K blobName : expectedBlobs) {
            AvroTestDataFixture.readAvroRecords(readBytes(blobName, compression))
                    .forEach(genericRecord -> actualValues.add(genericRecord.toString()));
        }

        final List<String> expectedValues = expectedRecords.stream()
                .map(GenericRecord::toString)
                .collect(Collectors.toList());
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    @Test
    void jsonlOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = CompressionType.NONE;

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.JSONL)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();

        for (final K nativeKey : expectedBlobs) {
            for (final JsonNode node : JsonTestDataFixture
                    .readJsonRecords(readBytes(nativeKey, CompressionType.NONE))) {
                actualValues.add(node.findValue("message").asText());
            }
        }

        final String[] expected = expectedGenericRecords.stream().map(mapF("message")).toArray(String[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expected);
    }
}
