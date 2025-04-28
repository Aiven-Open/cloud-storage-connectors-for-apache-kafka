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
    package io.aiven.kafka.connect.common.integration.sink;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


public abstract class AbstractByteParquetIntegrationTest<N, K extends Comparable<K>> extends AbstractSinkIntegrationTest<N, K> {
    private static final KafkaProducer<byte[], byte[]>  NULL_PRODUCER = null;
    private KafkaProducer<byte[], byte[]>  producer;
    @TempDir
    private static Path tmpDir;

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
    }

    /**
     * Creates a configuration with the storage and general Avro configuration options.
     * @param topics the topics to listend to.
     * @return ta configuration map.
     */
    @Override
    protected Map<String, String> createConfiguration(String... topics) {
        Map<String, String> config = super.createConfiguration(topics);
        KafkaFragment.setter(config)
                .valueConverter(ByteArrayConverter.class)
                .keyConverter(ByteArrayConverter.class);
        OutputFormatFragment.setter(config).withFormatType(FormatType.PARQUET);
        return config;
    }

    @Test
    void allOutputFields() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final String[] expectedFields = { "key", "value", "offset", "timestamp", "headers" };
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", String.join(",", expectedFields));
        connectorConfig.put("format.output.fields.value.encoding", "none");
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final List<String> expectedKeys = new ArrayList<>();
        final List<String> expectedValues = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                expectedKeys.add(key);
                final String value = "value-" + cnt;
                expectedValues.add(value);
                cnt += 1;
                final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topicName, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8));
                sendFutures.add(producer.send(msg));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);


        final List<String> actualKeys = new ArrayList<>();
        final List<String> actualValues = new ArrayList<>();
        for (final K blobName : expectedBlobs) {
            final List<GenericRecord> records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression));
            for (final GenericRecord record : records) {
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                actualKeys.add(new String(((ByteBuffer)record.get("key")).array()));
                actualValues.add(new String(((ByteBuffer)record.get("value")).array()));
            }
        }

        assertThat(actualKeys).containsExactlyInAnyOrderElementsOf(expectedKeys);
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    @Test
    void allOutputFieldsJsonValueAsString() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final String[] expectedFields = { "key", "value", "offset", "timestamp", "headers" };
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", String.join(",", expectedFields));
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final List<String> expectedKeys = new ArrayList<>();
        final List<String> expectedValues = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                expectedKeys.add(key);
                final String value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                expectedValues.add(value);
                cnt += 1;
                sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8))));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }


        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final List<String> actualKeys = new ArrayList<>();
        final List<String> actualValues = new ArrayList<>();
        for (final K blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression));
            for (final GenericRecord record : records) {
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                actualKeys.add(record.get("key").toString());
                actualValues.add(record.get("value").toString());
            }
        }

        assertThat(actualKeys).containsExactlyInAnyOrderElementsOf(expectedKeys);
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

@Test
void schemaChanged() throws ExecutionException, InterruptedException, IOException {
    final var topicName = getTopic();
    kafkaManager.createTopic(topicName);
    producer = newProducer();

    final String[] expectedFields = { "key", "value", "offset", "timestamp", "headers" };
    final CompressionType compression = CompressionType.NONE;
    final Map<String, String> connectorConfig = createConfiguration(topicName);
    connectorConfig.put("format.output.fields", "value");
    connectorConfig.put("format.output.fields.value.encoding", "none");
    connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    FileNameFragment.setter(connectorConfig).fileCompression(compression);

    kafkaManager.configureConnector(connectorName, connectorConfig);


    final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
    final var jsonMessageNewSchema = "{\"type\":\"struct\",\"fields\":"
            + "[{\"type\":\"string\",\"field\":\"name\"}, "
            + "{\"type\":\"string\",\"field\":\"value\", \"default\": \"foo\"}]}";
    final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

    final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
    final var expectedRecords = new ArrayList<String>();
    int cnt = 0;
    for (int i = 0; i < 10; i++) {
        for (int partition = 0; partition < 4; partition++) {
            final var key = "key-" + cnt;
            final String value;
            final String payload;
            if (i < 5) { // NOPMD literal
                payload = "{" + "\"name\": \"user-" + cnt + "\"}";
                value = String.format(jsonMessagePattern, jsonMessageSchema, payload);
            } else {
                payload = "{" + "\"name\": \"user-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                value = String.format(jsonMessagePattern, jsonMessageNewSchema, payload);
            }
            expectedRecords.add(String.format("{\"value\": %s}", payload));
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8))));
            cnt += 1;
        }
    }
    producer.flush();
    for (final Future<RecordMetadata> sendFuture : sendFutures) {
        sendFuture.get();
    }

    // get array of expected blobs
    final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
            sinkStorage.getBlobName(prefix, topicName, 0, 5, compression),
            sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
            sinkStorage.getBlobName(prefix, topicName, 1, 5, compression),
            sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
            sinkStorage.getBlobName(prefix, topicName, 2, 5, compression),
            sinkStorage.getBlobName(prefix, topicName, 3, 0, compression),
            sinkStorage.getBlobName(prefix, topicName, 3, 5, compression));

    final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

    waitForStorage(timeout, this::getNativeKeys, expectedBlobs);


    final var blobContents = new ArrayList<String>();
    for (final K blobName : expectedBlobs) {
        final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                readBytes(blobName, compression));
        blobContents.addAll(records.stream().map(GenericRecord::toString).collect(Collectors.toList()));
    }
    assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
}


    @ParameterizedTest
    @CsvSource({ "true" , "false"} )
    void envelopeTest(final String envelopeEnabledArg)
            throws ExecutionException, InterruptedException, IOException {

        boolean envelopeEnabled = Boolean.valueOf(envelopeEnabledArg);
        String valueFmt = envelopeEnabled ? "{\"value\": {\"name\": \"%s\"}}" : "{\"name\": \"%s\"}";
        final var topicName = getTopic(Boolean.toString(envelopeEnabled));
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                        .envelopeEnabled(envelopeEnabled)
                                .withOutputFields(OutputFieldType.VALUE)
                                        .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);


        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<String> expectedRecords = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String name = "user-" +cnt;
                expectedRecords.add(String.format(valueFmt, name));
                final String value = String.format(jsonMessagePattern, jsonMessageSchema,
                        "{" + "\"name\":\"user-" + cnt + "\"}");
                cnt += 1;

                sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8))));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final List<String> actualRecords = new ArrayList<>();
        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final K blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression));
            for (GenericRecord record : records) {
                actualRecords.add(record.toString());
            }
        }

        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    private KafkaProducer<byte[], byte[]> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

    /*
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
     */

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<KeyValueMessage> produceRecords(final int partitions, final int epochs,
                                                 final KeyValueGenerator keyValueGenerator, final String topicName)
            throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<KeyValueMessage> result = new ArrayList<>();

        for (final KeyValueMessage kvMsg : keyValueGenerator.generateMessages(partitions, epochs)) {
            result.add(kvMsg);
            sendFutures.add(producer.send(
                    new ProducerRecord<>(topicName, kvMsg.partition, kvMsg.getKeyBytes(), kvMsg.getValueBytes())));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return result;
    }

    private List<ProducerRecord<byte[], byte[]>> produceRecords(
            final Collection<ProducerRecord<byte[], byte[]>> records) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<ProducerRecord<byte[], byte[]>> result = new ArrayList<>();

        for (final ProducerRecord<byte[], byte[]> record : records) {
            result.add(record);
            sendFutures.add(producer.send(record));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return result;
    }

//    private List<>        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
//    int cnt = 0;
//        for (int i = 0; i < 10; i++) {
//        for (int partition = 0; partition < 4; partition++) {
//            final String key = "key-" + cnt;
//            final String value = "value-" + cnt;
//            cnt += 1;
//            final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topicName, partition, key.getBytes(StandardCharsets.UTF_8),
//                    value.getBytes(StandardCharsets.UTF_8));
//            sendFutures.add(producer.send(msg));
//        }
//    }
//        producer.flush();
//        for (final Future<RecordMetadata> sendFuture : sendFutures) {
//        sendFuture.get();
//    }


}
