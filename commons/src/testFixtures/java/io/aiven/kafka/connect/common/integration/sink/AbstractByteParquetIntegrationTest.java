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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.converters.ByteArrayConverter;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * parquet converter read/write parquet test
 *
 * @param <K>
 *            the native key type.
 * @param <N>
 *            The native storage type.
 */
public abstract class AbstractByteParquetIntegrationTest<K extends Comparable<K>, N>
        extends
            AbstractSinkIntegrationTest<K, N> {
    private static final KafkaProducer<byte[], byte[]> NULL_PRODUCER = null;
    private KafkaProducer<byte[], byte[]> producer;
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
     *
     * @param topics
     *            the topics to listend to.
     * @return ta configuration map.
     */
    @Override
    protected Map<String, String> createConfiguration(final String... topics) {
        final Map<String, String> config = super.createConfiguration(topics);
        KafkaFragment.setter(config).valueConverter(ByteArrayConverter.class).keyConverter(ByteArrayConverter.class);
        OutputFormatFragment.setter(config).withFormatType(FormatType.PARQUET);
        return config;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
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
                final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topicName, partition,
                        key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
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
            final List<GenericRecord> records = ParquetTestDataFixture
                    .readRecords(tmpDir.resolve(Paths.get(blobName.toString())), readBytes(blobName, compression));
            for (final GenericRecord record : records) {
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                actualKeys.add(new String(((ByteBuffer) record.get("key")).array(), StandardCharsets.UTF_8));
                actualValues.add(new String(((ByteBuffer) record.get("value")).array(), StandardCharsets.UTF_8));
            }
        }

        assertThat(actualKeys).containsExactlyInAnyOrderElementsOf(expectedKeys);
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
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
                sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition,
                        key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))));
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

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

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
                sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition,
                        key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))));
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

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @ParameterizedTest
    @CsvSource({ "true", "false" })
    void envelopeTest(final String envelopeEnabledArg) throws ExecutionException, InterruptedException, IOException {
        final boolean envelopeEnabled = Boolean.valueOf(envelopeEnabledArg);
        final String valueFmt = envelopeEnabled ? "{\"value\": {\"name\": \"%s\"}}" : "{\"name\": \"%s\"}";
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
                final String name = "user-" + cnt;
                expectedRecords.add(String.format(valueFmt, name));
                final String value = String.format(jsonMessagePattern, jsonMessageSchema,
                        "{" + "\"name\":\"user-" + cnt + "\"}");
                cnt += 1;

                sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition,
                        key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))));
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
        for (final K blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression));
            for (final GenericRecord record : records) {
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
}
