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
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


public abstract class AbstractParquetIntegrationTest <N, K extends Comparable<K>> extends AbstractSinkIntegrationTest<N, K> {

    private static final KafkaProducer<byte[], byte[]> NULL_PRODUCER = null;
    private KafkaProducer<byte[], byte[]> producer;


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
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        return config;
    }

    @Test
    final void allOutputFields(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression =sinkStorage.getDefaultCompression();

        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE, OutputFieldType.OFFSET, OutputFieldType.TIMESTAMP, OutputFieldType.HEADERS)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);


        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordsPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;

        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordsPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);


        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> expectedFields = Arrays.stream(OutputFieldType.values()).map(OutputFieldType::name).collect(Collectors.toList());
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");
        final long now = System.currentTimeMillis();
        for (final K blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression));
            int offset = 0;
            for (final GenericRecord r : lst) {
                final List<String> fields = r.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                assertThat(fields).containsExactlyInAnyOrderElementsOf(expectedFields);
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

        List<String> values = actualValues.stream().map(messageMapper).collect(Collectors.toList());
        List<String> expected = expectedRecords.stream()
                .map(KeyValueMessage::getValue)
                .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);

        values = actualValues.stream().map(idMapper).collect(Collectors.toList());
        expected = expectedRecords.stream().map(KeyValueMessage::getKey).collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    final void valueComplexType(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression =sinkStorage.getDefaultCompression();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");


        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int recordsPerPartition = 10;
        final int partitionCount = 4;

        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;

        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordsPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);


        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression), sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K blobName : expectedBlobs) {
            for (final GenericRecord r : ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(messageMapper).collect(Collectors.toList());
        List<String> expected = expectedRecords.stream()
                .map(KeyValueMessage::getValue)
                .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);

        values = actualValues.stream().map(idMapper).collect(Collectors.toList());
        expected = expectedRecords.stream().map(KeyValueMessage::getKey).collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);

    }

    @Test
    final void schemaChangedJson(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = sinkStorage.getDefaultCompression();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final int recordsBeforeSchemaChange = 5;
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int recordCountPerPartition = 10;
        final int partitionCount = 4;
        final int schemaChangeBoundary = recordsBeforeSchemaChange * partitionCount;
        final String[] expectedFieldsSchema1 = { "id", "message" };
        final String[] expectedFieldsSchema2 = { "id", "message", "age" };

        final String jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";


//        final Function<Integer, GenericRecord> recordGenerator = i -> {
//            GenericRecord value;
//            if (i < schemaChangeBoundary) {
//                value = new GenericData.Record(valueSchema); // NOPMD AvoidInstantiatingObjectsInLoops
//                value.put("name", "user-" + i);
//                value.put("value", Integer.toString(i));
//            } else {
//                value = new GenericData.Record(newValueSchema); // NOPMD AvoidInstantiatingObjectsInLoops
//                value.put("name", "user-" + i);
//                value.put("value", Integer.toString(i));
//                value.put("blocked", true);
//            }
//            return value;
//        };

        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> {
            final String payload = currIdx < schemaChangeBoundary
                    ? AvroTestDataFixture.formatDefaultData(currIdx, "Hello from partition " + partition)
                    : AvroTestDataFixture.formatEvolvedData(currIdx, "Hello from partition " + partition, epoch);
            final String schema = currIdx < schemaChangeBoundary ? AvroTestDataFixture.SCHEMA_JSON : AvroTestDataFixture.EVOLVED_SCHEMA_JSON;

            return String.format(jsonMessagePattern, schema, payload);
        };

        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordCountPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        // get array of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 0, 5, compression), sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 5, compression), sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 5, compression), sinkStorage.getBlobName(prefix, topicName, 3, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 5, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K blobName : expectedBlobs) {
            for (final GenericRecord r : ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName.toString())),
                    readBytes(blobName, compression))) {
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
                    assertThat(value.get("age")).isEqualTo(recordId);
                }
                actualValues.add(value);
            }
        }


        List<String> values = actualValues.stream().map(messageMapper).collect(Collectors.toList());
        List<String> expected = expectedRecords.stream()
                .map(KeyValueMessage::getValue)
                .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);

        values = actualValues.stream().map(idMapper).collect(Collectors.toList());
        expected = expectedRecords.stream()
                .map(KeyValueMessage::getKey)
                .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

//    private KafkaProducer<String, GenericRecord> newProducer() {
//        final Map<String, Object> producerProps = new HashMap<>();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        producerProps.put("schema.registry.url", kafkaManager.getSchemaRegistryUrl());
//        return new KafkaProducer<>(producerProps);
//    }

    private KafkaProducer<byte[], byte[]> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }


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

}
