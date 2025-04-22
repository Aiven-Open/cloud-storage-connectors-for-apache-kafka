/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.testutils.IndexesToString;
import io.aiven.kafka.connect.s3.testutils.KeyValueGenerator;
import io.aiven.kafka.connect.s3.testutils.KeyValueMessage;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class ParquetIntegrationTest extends AbstractKafkaIntegrationBase {
    private static final KafkaProducer<byte[], byte[]> NULL_PRODUCER = null;
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";

    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";

    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static String s3Endpoint;
    private static BucketAccessor testBucketAccessor;

    @TempDir
    Path tmpDir;

    @Container
    public static final LocalStackContainer LOCALSTACK = S3IntegrationHelper.createS3Container();

    private KafkaManager kafkaManager;
    private KafkaProducer<byte[], byte[]> producer;

    @BeforeAll
    static void setUpAll() {
        final AmazonS3 s3Client = S3IntegrationHelper.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
    }

    private Class<? extends Connector> getConnectorClass() {
        return AivenKafkaConnectS3SinkConnector.class;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        testBucketAccessor.createBucket();
        kafkaManager = setupKafka(getConnectorClass());
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
        testBucketAccessor.removeBucket();
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

    private Function<GenericRecord, String> mapF(final String key) {
        return r -> r.get(key).toString();
    }

    @Test
    void allOutputFields() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        final var compression = CompressionType.NONE;
        final int partitionCount = 4;
        final int recordsPerPartition = 10;
        final String[] expectedFields = { "key", "value", "offset", "timestamp", "headers" };

        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", String.join(",", expectedFields));
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordsPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        // get array of expected blobs
        final String[] expectedBlobs = { getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("key");
        final Function<GenericRecord, String> messageMapper = mapF("value");
        final long now = System.currentTimeMillis();
        for (final String blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            int offset = 0;
            for (final GenericRecord r : lst) {
                final List<String> fields = r.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                assertThat(fields).containsExactlyInAnyOrder(expectedFields);
                assertThat(messageMapper.apply(r)).endsWith(idMapper.apply(r));
                assertThat(Integer.parseInt(r.get("offset").toString())).isEqualTo(offset++);
                assertThat(r.get("timestamp")).isNotNull();
                assertThat(Long.parseLong(r.get("timestamp").toString())).isLessThan(now);
                assertThat(r.get("headers")).isNull();
                actualValues.add(r);
            }
        }

        List<String> values = actualValues.stream().map(mapF("value")).collect(Collectors.toList());
        String[] expected = expectedRecords.stream()
                .map(KeyValueMessage::getValue)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("key")).collect(Collectors.toList());
        expected = expectedRecords.stream()
                .map(KeyValueMessage::getKey)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

    }

    @Test
    void allOutputFieldsJsonValueAsString() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = CompressionType.NONE;
        final int partitionCount = 4;
        final int recordsPerPartition = 10;
        final String[] expectedFields = { "key", "value", "offset", "timestamp", "headers" };

        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", String.join(",", expectedFields));
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> JsonTestDataFixture.generateJsonRec(currIdx,
                "name-" + currIdx);
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordsPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        // get array of expected blobs
        final String[] expectedBlobs = { getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("key");
        final Function<GenericRecord, byte[]> messageMapper = mapF("value")
                .andThen(s -> s.getBytes(StandardCharsets.UTF_8));
        final long now = System.currentTimeMillis();
        for (final String blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            int offset = 0;
            for (final GenericRecord r : lst) {
                final List<String> fields = r.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                assertThat(fields).containsExactlyInAnyOrder(expectedFields);
                final JsonNode node = JsonTestDataFixture.readJsonRecord(messageMapper.apply(r));
                assertThat(node.get("id").asText()).isEqualTo(idMapper.apply(r));
                assertThat(node.get("value").asText()).isEqualTo("value" + idMapper.apply(r));
                assertThat(node.get("message").asText()).isEqualTo("name-" + idMapper.apply(r));

                assertThat(Integer.parseInt(r.get("offset").toString())).isEqualTo(offset++);
                assertThat(r.get("timestamp")).isNotNull();
                assertThat(Long.parseLong(r.get("timestamp").toString())).isLessThan(now);
                assertThat(r.get("headers")).isNull();

                actualValues.add(r);
            }
        }

        List<String> values = actualValues.stream().map(mapF("value")).collect(Collectors.toList());
        String[] expected = expectedRecords.stream()
                .map(KeyValueMessage::getValue)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("key")).collect(Collectors.toList());
        expected = expectedRecords.stream()
                .map(KeyValueMessage::getKey)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    @ParameterizedTest
    @CsvSource({ "true", "false" })
    void envelopeTest(final Boolean envelopeEnabled) throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic() + "-" + envelopeEnabled;
        final CompressionType compression = CompressionType.NONE;
        final int partitionCount = 4;
        final int recordsPerPartition = 10;
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.envelope", envelopeEnabled.toString());
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < recordsPerPartition; i++) {
            for (int partition = 0; partition < partitionCount; partition++) {
                final String key = "key-" + cnt;
                final String value = String.format(jsonMessagePattern, jsonMessageSchema,
                        "{" + "\"name\":\"user-" + cnt + "\"}");
                cnt += 1;

                producerRecords.add(new ProducerRecord<>(topicName, partition, bytesOrNull(key), bytesOrNull(value)));
            }
        }
        produceRecords(producerRecords);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final String[] expectedBlobs = { getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        for (final String blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            for (final GenericRecord r : lst) {
                if (envelopeEnabled) {
                    assertThat(r.hasField("value")).isTrue();
                } else {
                    assertThat(r.hasField("value")).isFalse();
                }
            }
        }
    }

    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = CompressionType.NONE;

        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final String jsonMessageSchema1 = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"id\"},"
                + "{\"type\":\"string\",\"field\":\"value\"}]}";
        final String jsonMessageSchema2 = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"id\"},"
                + "{\"type\":\"string\",\"field\":\"value\"}, {\"type\":\"string\",\"field\":\"message\", \"default\": \"no message\"}]}";
        final String jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final int recordsBeforeSchemaChange = 5;
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int recordCountPerPartition = 10;
        final int partitionCount = 4;
        final int schemaChangeBoundary = recordsBeforeSchemaChange * partitionCount;
        final String[] expectedFieldsSchema1 = { "id", "value" };
        final String[] expectedFieldsSchema2 = { "id", "value", "message" };

        final String schemaFmt1 = "{\"id\" : \"%1$s\", \"value\" : \"value%1$s\"}%n";
        final String schemaFmt2 = "{\"id\" : \"%s\", \"message\" : \"from partition %s epoch %s\", \"value\" : \"value%s\"}%n";
        final IndexesToString keyGen = (partition, epoch, currIdx) -> Integer.toString(currIdx);
        final IndexesToString valueGen = (partition, epoch, currIdx) -> {
            final String payload = currIdx < schemaChangeBoundary
                    ? String.format(schemaFmt1, currIdx)
                    : String.format(schemaFmt2, currIdx, partition, epoch, currIdx);
            final String schema = currIdx < schemaChangeBoundary ? jsonMessageSchema1 : jsonMessageSchema2;

            return String.format(jsonMessagePattern, schema, payload);
        };

        final List<KeyValueMessage> expectedRecords = produceRecords(partitionCount, recordCountPerPartition,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        final String[] expectedBlobs = { getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 0, 5, compression), getBlobName(topicName, 1, 0, compression),
                getBlobName(topicName, 1, 5, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 2, 5, compression), getBlobName(topicName, 3, 0, compression),
                getBlobName(topicName, 3, 5, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> valueMapper = mapF("value");
        final List<GenericRecord> actualValues = new ArrayList<>();

        for (final String blobName : expectedBlobs) {
            final List<GenericRecord> lst = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            for (final GenericRecord r : lst) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final List<String> fields = value.getSchema()
                        .getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());

                final int recordId = Integer.parseInt(idMapper.apply(value));
                if (recordId < schemaChangeBoundary) {
                    assertThat(fields).containsExactlyInAnyOrder(expectedFieldsSchema1);
                } else {
                    assertThat(fields).containsExactlyInAnyOrder(expectedFieldsSchema2);
                    assertThat(value.get("message")).isNotEqualTo("no message");
                }
                assertThat(valueMapper.apply(value)).isEqualTo("value" + idMapper.apply(value));
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(valueMapper).collect(Collectors.toList());
        String[] expected = expectedRecords.stream().map(kvm -> {
            try {
                return JsonTestDataFixture.readJsonRecord(kvm.getValueBytes()).get("payload").get("value").asText();
            } catch (IOException e) {
                return fail(e);
            }
        }).collect(Collectors.toList()).toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(idMapper).collect(Collectors.toList());
        expected = expectedRecords.stream().map(kvm -> {
            try {
                return JsonTestDataFixture.readJsonRecord(kvm.getValueBytes()).get("payload").get("id").asText();
            } catch (IOException e) {
                return fail(e);
            }
        }).collect(Collectors.toList()).toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final String topicName) {
        return awsSpecificConfig(config, List.of(topicName));
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final List<String> topicNames) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("topics", String.join(",", topicNames));
        return config;
    }

    private Map<String, String> basicConnectorConfig(final CompressionType compression) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", getConnectorName(getConnectorClass()));
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        config.put("file.compression.type", compression.name());
        config.put("format.output.type", "parquet");
        return config;
    }

    private String getBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s-%d-%d", topicName, partition, startOffset);
        return result + compression.extension();
    }

}
