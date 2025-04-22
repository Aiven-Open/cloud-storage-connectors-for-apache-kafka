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

package io.aiven.kafka.connect;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.request;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.testutils.IndexesToString;
import io.aiven.kafka.connect.s3.testutils.KeyValueGenerator;
import io.aiven.kafka.connect.s3.testutils.KeyValueMessage;

import com.amazonaws.services.s3.AmazonS3;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class IntegrationTest extends AbstractKafkaIntegrationBase {
    private static final KafkaProducer<byte[], byte[]> NULL_PRODUCER = null;
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String COMMON_PREFIX = "s3-connector-for-apache-kafka-test-";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    @Container
    public static final LocalStackContainer LOCALSTACK = S3IntegrationHelper.createS3Container();

    private KafkaProducer<byte[], byte[]> producer;
    private KafkaManager kafkaManager;

    @BeforeAll
    static void setUpAll() {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

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

    public static List<Arguments> compressionTypes() {
        final List<Arguments> compressionTypes = new ArrayList<>();
        for (final CompressionType compressionType : CompressionType.values()) {
            compressionTypes.add(Arguments.of(compressionType));
        }
        return compressionTypes;
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

    private List<ProducerRecord<byte[], byte[]>> produceRecords(final ProducerRecord<byte[], byte[]>... records)
            throws ExecutionException, InterruptedException {
        return produceRecords(Arrays.asList(records));
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

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void basicTest(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("aws.s3.prefix", s3Prefix);

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitions = 4;
        final int epochs = 10;

        final List<KeyValueMessage> expectedRecords = produceRecords(partitions, epochs,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        // get array of expected blobs
        final String[] expectedBlobs = { getOldBlobName(topicName, 0, 0, compression),
                getOldBlobName(topicName, 1, 0, compression), getOldBlobName(topicName, 2, 0, compression),
                getOldBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        // read the data
        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();

        for (final String blobName : expectedBlobs) {
            testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).forEach(fields -> {
                keys.add(fields.get(0));
                values.add(fields.get(1));
            });
        }

        String[] expected = expectedRecords.stream().map(kvm -> kvm.key).toArray(String[]::new);
        assertThat(keys).containsExactlyInAnyOrder(expected);

        expected = expectedRecords.stream().map(kvm -> kvm.value).toArray(String[]::new);
        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void groupByTimestampVariable(final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", s3Prefix + "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        produceRecords(recordOf(topicName, 0, "key-0", "value-0"), recordOf(topicName, 0, "key-1", "value-1"),
                recordOf(topicName, 0, "key-2", "value-2"), recordOf(topicName, 1, "key-3", "value-3"),
                recordOf(topicName, 3, "key-4", "value-4"));

        // get expected blobs
        final Map<String, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getTimestampBlobName(topicName, 0, 0),
                new String[] { "key-0,value-0", "key-1,value-1", "key-2,value-2" });
        expectedBlobsAndContent.put(getTimestampBlobName(topicName, 1, 0), new String[] { "key-3,value-3" });
        expectedBlobsAndContent.put(getTimestampBlobName(topicName, 3, 0), new String[] { "key-4,value-4" });

        final String[] expectedBlobs = expectedBlobsAndContent.keySet().toArray(String[]::new);

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        for (final String blobName : expectedBlobs) {
            final List<String> blobContent = testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.toList());
            assertThat(blobContent).containsExactlyInAnyOrder(expectedBlobsAndContent.get(blobName));
        }
    }

    private String getTimestampBlobName(final String topicName, final int partition, final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s-%s-%s", s3Prefix, topicName, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")), time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd")));
    }

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void oneFilePerRecordWithPlainValues(final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        produceRecords(recordOf(topicName, 0, "key-0", "value-0"), recordOf(topicName, 0, "key-1", "value-1"),
                recordOf(topicName, 0, "key-2", "value-2"), recordOf(topicName, 1, "key-3", "value-3"),
                recordOf(topicName, 3, "key-4", "value-4"));

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 3, 0, compression), "value-4");

        final String[] expectedBlobs = expectedBlobsAndContent.keySet().toArray(String[]::new);

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        for (final Map.Entry<String, String> blobAndContentEntry : expectedBlobsAndContent.entrySet()) {
            assertThat(testBucketAccessor.readLines(blobAndContentEntry.getKey(), compression).get(0))
                    .isEqualTo(blobAndContentEntry.getValue());
        }
    }

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void groupByKey(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final String topicName0 = getTopic() + "-" + compression.name();
        final String topicName1 = topicName0 + "_1";
        final List<String> topics = List.of(topicName0, topicName1);

        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topics);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", s3Prefix + "{{key}}" + compression.extension());

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopics(List.of(topicName0, topicName1));
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(new TopicPartition(topicName0, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3"));
        keysPerTopicPartition.put(new TopicPartition(topicName0, 1), Arrays.asList("key-4", "key-5", "key-6"));
        keysPerTopicPartition.put(new TopicPartition(topicName1, 0), Arrays.asList(null, "key-7"));
        keysPerTopicPartition.put(new TopicPartition(topicName1, 1), List.of("key-8"));

        final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        final Map<String, String> lastValuePerKey = new HashMap<>();
        final int numValuesPerKey = 10;
        final int cntMax = 10 * numValuesPerKey;
        int cnt = 0;
        outer : while (true) {
            for (final Map.Entry<TopicPartition, List<String>> topicPartitionListEntry : keysPerTopicPartition
                    .entrySet()) {
                final TopicPartition topicPartition = topicPartitionListEntry.getKey();
                for (final String key : topicPartitionListEntry.getValue()) {
                    final String value = "value-" + cnt;
                    cnt += 1;
                    records.add(recordOf(topicPartition.topic(), topicPartition.partition(), key, value));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }
        produceRecords(records);

        final String[] expectedBlobs = keysPerTopicPartition.values()
                .stream()
                .flatMap(keys -> keys.stream().map(k -> getKeyBlobName(k, compression)))
                .toArray(String[]::new);

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        for (final String blobName : expectedBlobs) {
            final String blobContent = testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());
            final String keyInBlobName = blobName.replace(s3Prefix, "").replace(compression.extension(), "");
            final String value;
            final String expectedBlobContent;
            if ("null".equals(keyInBlobName)) {
                value = lastValuePerKey.get(null);
                expectedBlobContent = String.format("%s,%s", "", value);
            } else {
                value = lastValuePerKey.get(keyInBlobName);
                expectedBlobContent = String.format("%s,%s", keyInBlobName, value);
            }
            assertThat(blobContent).isEqualTo(expectedBlobContent);
        }
    }

    @Test
    void jsonlOutputTest() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;
                records.add(recordOf(topicName, partition, key, value));
            }
        }
        produceRecords(records);

        final String[] expectedBlobs = { getNewBlobName(topicName, 0, 0, compression),
                getNewBlobName(topicName, 1, 0, compression), getNewBlobName(topicName, 2, 0, compression),
                getNewBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = Collections
                    .unmodifiableList(testBucketAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                final String blobName = getNewBlobName(topicName, partition, 0, compression);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";

                assertThat(blobContents.get(blobName).get(i)).isEqualTo(expectedLine);
            }
        }
    }

    @Test
    void jsonOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        final String contentType = "json";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        final int epochs = 10;
        final int partitions = 4;

        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "[{" + "\"name\":\"user-" + currIdx + "\"}]";

        final List<KeyValueMessage> expectedRecords = produceRecords(partitions, epochs,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        final String[] expectedBlobs = { getNewBlobName(topicName, 0, 0, compression),
                getNewBlobName(topicName, 1, 0, compression), getNewBlobName(topicName, 2, 0, compression),
                getNewBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = testBucketAccessor.readLines(blobName, compression);
            assertThat(items).hasSize(epochs + 2);
            blobContents.put(blobName, Collections.unmodifiableList(items));
        }

        // Each blob must be a JSON array.
        for (final KeyValueMessage msg : expectedRecords) {
            final String blobName = getNewBlobName(topicName, msg.partition, 0, compression);
            final List<String> blobContent = blobContents.get(blobName);

            assertThat(blobContent.get(0)).isEqualTo("[");
            assertThat(blobContent.get(blobContent.size() - 1)).isEqualTo("]");

            final String actualLine = blobContent.get(msg.epoch + 1); // 0 is '['

            final StringBuilder expectedLine = new StringBuilder( // NOPMD AvoidInstantiatingObjectsInLoops
                    "{\"value\":" + msg.value + ",\"key\":\"" + msg.key + "\"}");
            if (actualLine.endsWith(",")) {
                expectedLine.append(',');
            }
            assertThat(actualLine).isEqualTo(expectedLine.toString());
        }
    }

    @Test
    void errorRecovery() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = CompressionType.NONE;
        final var faultyProxy = enableFaultyProxy(topicName);
        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("aws.s3.endpoint", faultyProxy.baseUrl());
        final String contentType = "json";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 4);

        final int epochs = 10;
        final int partitions = 4;

        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "[{" + "\"name\":\"user-" + currIdx + "\"}]";

        final List<KeyValueMessage> expectedRecords = produceRecords(partitions, epochs,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        final String[] expectedBlobs = { getNewBlobName(topicName, 0, 0, compression),
                getNewBlobName(topicName, 1, 0, compression), getNewBlobName(topicName, 2, 0, compression),
                getNewBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = testBucketAccessor.readLines(blobName, compression);
            assertThat(items).hasSize(epochs + 2);
            blobContents.put(blobName, Collections.unmodifiableList(items));
        }

        // Each blob must be a JSON array.
        for (final KeyValueMessage msg : expectedRecords) {
            final String blobName = getNewBlobName(topicName, msg.partition, 0, compression);
            final List<String> blobContent = blobContents.get(blobName);

            assertThat(blobContent.get(0)).isEqualTo("[");
            assertThat(blobContent.get(blobContent.size() - 1)).isEqualTo("]");

            final String actualLine = blobContent.get(msg.epoch + 1); // 0 is '['

            final StringBuilder expectedLine = new StringBuilder( // NOPMD AvoidInstantiatingObjectsInLoops
                    "{\"value\":" + msg.value + ",\"key\":\"" + msg.key + "\"}");
            if (actualLine.endsWith(",")) {
                expectedLine.append(',');
            }
            assertThat(actualLine).isEqualTo(expectedLine.toString());
        }
    }

    private static WireMockServer enableFaultyProxy(final String topicName) {
        System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicHttpsPort());
        wireMockServer.start();
        wireMockServer.addStubMapping(
                request(RequestMethod.ANY.getName(), UrlPattern.ANY).willReturn(aResponse().proxiedFrom(s3Endpoint))
                        .build());
        final String urlPathPattern = "/" + TEST_BUCKET_NAME + "/" + topicName + "([\\-0-9]+)";
        wireMockServer.addStubMapping(
                request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                        .inScenario("temp-error")
                        .willSetStateTo("Error")
                        .willReturn(aResponse().withStatus(400))
                        .build());
        wireMockServer.addStubMapping(
                request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                        .inScenario("temp-error")
                        .whenScenarioStateIs("Error")
                        .willReturn(aResponse().proxiedFrom(s3Endpoint))
                        .build());
        return wireMockServer;
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

    private Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", connectorName);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        return config;
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

    // WARN: different from GCS
    private String getOldBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    private String getNewBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s-%d-%d", topicName, partition, startOffset);
        return result + compression.extension();
    }

    private String getKeyBlobName(final String key, final CompressionType compression) {
        final String result = String.format("%s%s", s3Prefix, key);
        return result + compression.extension();
    }
}
