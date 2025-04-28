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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;

import io.confluent.connect.avro.AvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro converter read/write parquet test
 *
 * @param <N>
 *            The native storage type.
 * @param <K>
 *            the native key type.
 */
public abstract class AbstractAvroParquetIntegrationTest<N, K extends Comparable<K>>
        extends
            AbstractSinkIntegrationTest<N, K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAvroParquetIntegrationTest.class);
    @TempDir
    private static Path tmpDir;
    private static final KafkaProducer<String, GenericRecord> NULL_PRODUCER = null;
    private KafkaProducer<String, GenericRecord> producer;

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
    }

    /**
     * Creates producer for avro converter read/write
     *
     * @return
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
        KafkaFragment.setter(config).valueConverter(AvroConverter.class).keyConverter(AvroConverter.class);
        OutputFormatFragment.setter(config).withFormatType(FormatType.PARQUET);
        return config;
    }

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
            final ProducerRecord<String, GenericRecord> rec = new ProducerRecord<>(topicName, partition, key, value);
            sendFutures.add(producer.send(rec, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                    if (exception != null) {
                        LOGGER.error("Error writing {}", rec, exception);
                    } else {
                        LOGGER.info("wrote metadata: {}", metadata);
                    }
                }
            }));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
    }

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

    @Test
    final void allOutputFields(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {

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
        final int recordCountPerPartition = 10;
        final int partitionCount = 4;
        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");
        final long now = System.currentTimeMillis();
        for (final K blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture
                    .readRecords(tmpDir.resolve(Paths.get(blobName.toString())), readBytes(blobName, compression));
            int offset = 0;
            for (final GenericRecord r : lst) {
                final List<String> fields = r.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                assertThat(fields).containsExactlyInAnyOrder(expectedFields);
                final GenericRecord value = (GenericRecord) r.get("value");
                // verify that additional fields were added.
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                assertThat(Integer.parseInt(r.get("offset").toString())).isEqualTo(offset++);
                assertThat(r.get("timestamp")).isNotNull();
                assertThat(Long.parseLong(r.get("timestamp").toString())).isLessThan(now);
                assertThat(r.get("headers")).isNull();
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
    final void valueComplexType(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int recordCountPerPartition = 10;
        final int partitionCount = 4;
        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K blobName : expectedBlobs) {
            for (final GenericRecord r : ParquetTestDataFixture
                    .readRecords(tmpDir.resolve(Paths.get(blobName.toString())), readBytes(blobName, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
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
    final void schemaChanged(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Schema valueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .endRecord();

        final Schema newValueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .name("blocked")
                .type()
                .booleanType()
                .booleanDefault(false)
                .endRecord();

        final int recordsBeforeSchemaChange = 5;
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int recordCountPerPartition = 10;
        final int partitionCount = 4;
        final int schemaChangeBoundary = recordsBeforeSchemaChange * partitionCount;
        final String[] expectedFieldsSchema1 = { "name", "value" };
        final String[] expectedFieldsSchema2 = { "name", "value", "blocked" };

        final Function<Integer, GenericRecord> recordGenerator = i -> {
            GenericRecord value;
            if (i < schemaChangeBoundary) {
                value = new GenericData.Record(valueSchema); // NOPMD AvoidInstantiatingObjectsInLoops
                value.put("name", "user-" + i);
                value.put("value", Integer.toString(i));
            } else {
                value = new GenericData.Record(newValueSchema); // NOPMD AvoidInstantiatingObjectsInLoops
                value.put("name", "user-" + i);
                value.put("value", Integer.toString(i));
                value.put("blocked", true);
            }
            return value;
        };

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName, recordGenerator);

        // get expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 0, 5, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 5, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 5, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 5, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("value");
        final Function<GenericRecord, String> messageMapper = mapF("name");

        for (final K blobName : expectedBlobs) {
            for (final GenericRecord r : ParquetTestDataFixture
                    .readRecords(tmpDir.resolve(Paths.get(blobName.toString())), readBytes(blobName, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                // verify the schema change.
                final int recordId = Integer.parseInt(idMapper.apply(value));
                final List<String> fields = value.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                if (recordId < schemaChangeBoundary) {
                    assertThat(fields).containsExactlyInAnyOrder(expectedFieldsSchema1);
                } else {
                    assertThat(fields).containsExactlyInAnyOrder(expectedFieldsSchema2);
                    assertThat(value.get("blocked")).isEqualTo(true);
                }
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(messageMapper).collect(Collectors.toList());
        String[] expected = expectedGenericRecords.stream()
                .map(messageMapper)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("value")).collect(Collectors.toList());
        expected = expectedGenericRecords.stream()
                .map(mapF("value"))
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

}
