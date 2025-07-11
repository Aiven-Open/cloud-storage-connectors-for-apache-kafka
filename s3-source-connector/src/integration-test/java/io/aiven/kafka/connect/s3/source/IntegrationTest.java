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

package io.aiven.kafka.connect.s3.source;

import static io.aiven.kafka.connect.common.config.CommonConfig.MAX_TASKS;
import static io.aiven.kafka.connect.common.config.FileNameFragment.FILE_NAME_TEMPLATE_CONFIG;
import static io.aiven.kafka.connect.common.config.FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC;
import static io.aiven.kafka.connect.common.config.TransformerFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.TransformerFragment.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.common.config.TransformerFragment.TRANSFORMER_MAX_BUFFER_SIZE;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_FETCH_BUFFER_SIZE;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.ParquetTestingFixture;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.plexus.util.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Testcontainers
@SuppressWarnings("PMD.ExcessiveImports")
final class IntegrationTest implements IntegrationBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);
    private static final String CONNECTOR_NAME = "s3-source-connector";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 500;

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
    private SchemaRegistryContainer schemaRegistry;

    private AdminClient adminClient;
    private ConnectRunner connectRunner;

    private static S3Client s3Client;
    private TestInfo testInfo;

    @Override
    public S3Client getS3Client() {
        return s3Client;
    }

    @Override
    public String getS3Prefix() {
        return s3Prefix;
    }

    @BeforeAll
    public static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);

        final Path pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws Exception {
        testBucketAccessor.createBucket();
        this.testInfo = testInfo;

        connectRunner = new ConnectRunner(OFFSET_FLUSH_INTERVAL_MS);
        final List<Integer> ports = IntegrationBase.getKafkaListenerPorts();
        final int localListenerPort = ports.get(0);
        final int containerListenerPort = ports.get(1);
        connectRunner.startConnectCluster(CONNECTOR_NAME, localListenerPort, containerListenerPort);

        adminClient = newAdminClient(connectRunner.getBootstrapServers());
        final String topic = IntegrationBase.getTopic(testInfo);
        final var topics = List.of(topic);
        IntegrationBase.createTopics(adminClient, topics);

        // This should be done after the process listening the port is already started by host but
        // before the container that will access it is started.
        org.testcontainers.Testcontainers.exposeHostPorts(containerListenerPort);
        schemaRegistry = new SchemaRegistryContainer("host.testcontainers.internal:" + containerListenerPort);
        schemaRegistry.start();
        IntegrationBase.waitForRunningContainer(schemaRegistry);
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
        connectRunner.deleteConnector(CONNECTOR_NAME);
        connectRunner.stopConnectCluster();
        schemaRegistry.stop();
        testBucketAccessor.removeBucket();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void bytesTest(final boolean addPrefix) {

        final var topic = IntegrationBase.getTopic(testInfo);
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";
        String localS3Prefix = null;
        if (addPrefix) {
            localS3Prefix = "topics/" + topic + "/partition=" + partitionId + "/";
        }

        final String fileNamePatternSeparator = "-";

        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 1, DistributionType.PARTITION,
                addPrefix, localS3Prefix, prefixPattern, fileNamePatternSeparator);
        connectorConfig.put("aws.s3.fetch.buffer.size", "10");
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<String> offsetKeys = new ArrayList<>();

        // write 5 objects to s3 test non padded partitions
        offsetKeys.add(writeToS3(topic, testData1.getBytes(StandardCharsets.UTF_8), "0", localS3Prefix));
        offsetKeys.add(writeToS3(topic, testData2.getBytes(StandardCharsets.UTF_8), "0", localS3Prefix));
        offsetKeys.add(writeToS3(topic, testData1.getBytes(StandardCharsets.UTF_8), "1", localS3Prefix));
        offsetKeys.add(writeToS3(topic, testData2.getBytes(StandardCharsets.UTF_8), "1", localS3Prefix));
        offsetKeys.add(writeToS3(topic, new byte[0], "3"));

        // Start the Connector
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        assertThat(testBucketAccessor.listObjects()).hasSize(5);
        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeByteMessages(topic, 4, connectRunner.getBootstrapServers());

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).containsOnly(testData1, testData2);

        // Verify offset positions
        final Map<String, Long> expectedOffsetRecords = offsetKeys.subList(0, offsetKeys.size() - 1)
                .stream()
                .collect(Collectors.toMap(Function.identity(), s -> 1L));
        verifyOffsetPositions(expectedOffsetRecords, connectRunner.getBootstrapServers());
        // add keys we haven't processed before
        offsetKeys.add("s3-object-key-from-bucket");
        offsetKeys.add("topic-one/s3-object-key-from-bucket");
        verifyOffsetsConsumeableByS3OffsetMgr(connectorConfig, offsetKeys, expectedOffsetRecords);
    }

    @ParameterizedTest
    @CsvSource({ "4096,'',object_hash", "3000,'',object_hash", "4101,'',object_hash",
            "4113,'{{topic}}-{{partition}}-.*',partition", "4113,'.*',object_hash" })
    void bytesBufferTest(final int maxBufferSize, final String overrideFileNameTemplate,
            final String distributionTypeName) {
        final var topic = IntegrationBase.getTopic(testInfo);
        final DistributionType distributionType = DistributionType.forName(distributionTypeName);

        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 1, distributionType, false, null,
                null, "-");

        if (StringUtils.isNotBlank(overrideFileNameTemplate)) {
            connectorConfig.put(FILE_NAME_TEMPLATE_CONFIG, overrideFileNameTemplate);
        }

        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        connectorConfig.put(TRANSFORMER_MAX_BUFFER_SIZE, String.valueOf(maxBufferSize));
        connectorConfig.put(AWS_S3_FETCH_BUFFER_SIZE, String.valueOf(10));
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        final int byteArraySize = 6000;
        final byte[] testData1 = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData1[i] = ((Integer) i).byteValue();
        }
        final List<String> offsetKeys = new ArrayList<>();

        offsetKeys.add(writeToS3(topic, testData1, "0"));

        assertThat(testBucketAccessor.listObjects()).hasSize(1);

        // Poll messages from the Kafka topic and verify the consumed data
        final List<byte[]> records = IntegrationBase.consumeRawByteMessages(topic, 2,
                connectRunner.getBootstrapServers());

        assertThat(records.get(0)).hasSize(maxBufferSize);
        assertThat(records.get(1)).hasSize(byteArraySize - maxBufferSize);

        assertThat(records.get(0)).isEqualTo(Arrays.copyOfRange(testData1, 0, maxBufferSize));
        assertThat(records.get(1)).isEqualTo(Arrays.copyOfRange(testData1, maxBufferSize, testData1.length));

        // Verify offset positions
        final Map<String, Long> expectedOffsetRecords = offsetKeys.stream()
                .collect(Collectors.toMap(Function.identity(), s -> 2L));
        verifyOffsetPositions(expectedOffsetRecords, connectRunner.getBootstrapServers());
    }

    @Test
    void avroTest(final TestInfo testInfo) throws IOException {
        final var topic = IntegrationBase.getTopic(testInfo);
        final boolean addPrefix = false;
        final Map<String, String> connectorConfig = getAvroConfig(topic, InputFormat.AVRO, addPrefix, "", "",
                DistributionType.OBJECT_HASH);

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        // Define Avro schema
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        final int numOfRecsFactor = 5000;

        final byte[] outputStream1 = IntegrationBase.generateNextAvroMessagesStartingFromId(1, numOfRecsFactor, schema);
        final byte[] outputStream2 = IntegrationBase.generateNextAvroMessagesStartingFromId(numOfRecsFactor + 1,
                numOfRecsFactor, schema);
        final byte[] outputStream3 = IntegrationBase.generateNextAvroMessagesStartingFromId(2 * numOfRecsFactor + 1,
                numOfRecsFactor, schema);
        final byte[] outputStream4 = IntegrationBase.generateNextAvroMessagesStartingFromId(3 * numOfRecsFactor + 1,
                numOfRecsFactor, schema);
        final byte[] outputStream5 = IntegrationBase.generateNextAvroMessagesStartingFromId(4 * numOfRecsFactor + 1,
                numOfRecsFactor, schema);

        final Set<String> offsetKeys = new HashSet<>();
        // test padded partition ids
        offsetKeys.add(writeToS3(topic, outputStream1, "00001"));
        offsetKeys.add(writeToS3(topic, outputStream2, "00001"));

        offsetKeys.add(writeToS3(topic, outputStream3, "00002"));
        offsetKeys.add(writeToS3(topic, outputStream4, "00002"));
        offsetKeys.add(writeToS3(topic, outputStream5, "00002"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        // Poll Avro messages from the Kafka topic and deserialize them
        // Waiting for 25k kafka records in this test so a longer Duration is added.
        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topic, numOfRecsFactor * 5,
                Duration.ofMinutes(3), connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl());
        // Ensure this method deserializes Avro

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).map(record -> entry(record.get("id"), String.valueOf(record.get("message"))))
                .contains(entry(1, "Hello, Kafka Connect S3 Source! object 1"),
                        entry(2, "Hello, Kafka Connect S3 Source! object 2"),
                        entry(numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + numOfRecsFactor),
                        entry(2 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (2 * numOfRecsFactor)),
                        entry(3 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (3 * numOfRecsFactor)),
                        entry(4 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (4 * numOfRecsFactor)),
                        entry(5 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (5 * numOfRecsFactor)));

        final Map<String, Long> expectedRecords = offsetKeys.stream()
                .collect(Collectors.toMap(Function.identity(), s -> (long) numOfRecsFactor));
        verifyOffsetPositions(expectedRecords, connectRunner.getBootstrapServers());
        verifyOffsetsConsumeableByS3OffsetMgr(connectorConfig, offsetKeys, expectedRecords);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void parquetTest(final boolean addPrefix) throws IOException {
        final var topic = IntegrationBase.getTopic(testInfo);

        final String partition = "0";
        final String prefixPattern = "bucket/topics/{{topic}}/partition/{{partition}}/";

        String localS3Prefix = "";
        if (addPrefix) {
            localS3Prefix = "bucket/topics/" + topic + "/partition/" + partition + "/";
        }

        final String fileName = localS3Prefix + topic + "-" + partition + "-" + System.currentTimeMillis() + ".txt";
        final String name = "testuser";

        final Map<String, String> connectorConfig = getAvroConfig(topic, InputFormat.PARQUET, addPrefix, localS3Prefix,
                prefixPattern, DistributionType.PARTITION);
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        final Path parquetFileDir = Files.createTempDirectory("parquet_tests");
        final Path path = ParquetTestingFixture.writeParquetFile(parquetFileDir.resolve("users.parquet"), name);

        try {
            s3Client.putObject(PutObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(fileName).build(), path);
        } catch (final Exception e) { // NOPMD broad exception caught
            LOGGER.error("Error in reading file {}", e.getMessage(), e);
        } finally {
            Files.delete(path);
        }

        // Waiting for a small number of messages so using a smaller Duration of a minute
        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topic, 100, Duration.ofSeconds(60),
                connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl());
        final List<String> expectedRecordNames = IntStream.range(0, 100)
                .mapToObj(i -> name + i)
                .collect(Collectors.toList());
        assertThat(records).extracting(record -> record.get("name").toString())
                .containsExactlyInAnyOrderElementsOf(expectedRecordNames);
    }

    private Map<String, String> getAvroConfig(final String topic, final InputFormat inputFormat,
            final boolean addPrefix, final String s3Prefix, final String prefixPattern,
            final DistributionType distributionType) {
        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 4, distributionType, addPrefix,
                s3Prefix, prefixPattern, "-");

        connectorConfig.put(INPUT_FORMAT_KEY, inputFormat.getValue());
        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
        TransformerFragment.setter(connectorConfig).valueConverterSchemaRegistry(schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        return connectorConfig;
    }

    @Test
    void jsonTest(final TestInfo testInfo) {

        final var topic = IntegrationBase.getTopic(testInfo);
        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 1, DistributionType.PARTITION,
                false, "", "", "-");
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.JSONL.getValue());
        connectorConfig.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.json.JsonConverter");

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);
        final String testMessage = "This is a test ";
        final StringBuilder jsonBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            final String jsonContent = "{\"message\": \"" + testMessage + "\", \"id\":\"" + i + "\"}";
            jsonBuilder.append(jsonContent).append("\n"); // NOPMD
        }
        final byte[] jsonBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);

        final String offsetKey = writeToS3(topic, jsonBytes, "00001");

        // Poll Json messages from the Kafka topic and deserialize them
        final List<JsonNode> records = IntegrationBase.consumeJsonMessages(topic, 500,
                connectRunner.getBootstrapServers());

        assertThat(records).map(jsonNode -> jsonNode.get("payload")).anySatisfy(jsonNode -> {
            assertThat(jsonNode.get("message").asText()).contains(testMessage);
            assertThat(jsonNode.get("id").asText()).contains("1");
        });

        // Verify offset positions
        final Map<String, Long> expectedRecords = Map.of(offsetKey, 500L);
        verifyOffsetPositions(expectedRecords, connectRunner.getBootstrapServers());
        // Fake key 1 and 2 will not be populated into the offset manager.
        verifyOffsetsConsumeableByS3OffsetMgr(connectorConfig, List.of(offsetKey, "fake-key-1", "fake-key-2"),
                expectedRecords);
    }

    private Map<String, String> getConfig(final String connectorName, final String topics, final int maxTasks,
            final DistributionType taskDistributionConfig, final boolean addPrefix, final String s3Prefix,
            final String prefixPattern, final String fileNameSeparator) {
        final Map<String, String> config = new HashMap<>(basicS3ConnectorConfig(addPrefix, s3Prefix));
        config.put("name", connectorName);
        config.put(TARGET_TOPIC, topics);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(MAX_TASKS, String.valueOf(maxTasks));
        SourceConfigFragment.setter(config).distributionType(taskDistributionConfig);
        config.put(FILE_NAME_TEMPLATE_CONFIG,
                "{{topic}}" + fileNameSeparator + "{{partition}}" + fileNameSeparator + "{{start_offset}}");
        if (addPrefix) {
            config.put(FILE_PATH_PREFIX_TEMPLATE_CONFIG, prefixPattern);
        }
        return config;
    }

    private static Map<String, String> basicS3ConnectorConfig(final boolean addPrefix, final String s3Prefix) {
        final Map<String, String> config = new HashMap<>();
        config.put("connector.class", S3SourceConnector.class.getName());
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        if (addPrefix) {
            config.put(AWS_S3_PREFIX_CONFIG, s3Prefix);
        }
        return config;
    }

    static void verifyOffsetPositions(final Map<String, Long> expectedRecords, final String bootstrapServers) {
        final Properties consumerProperties = IntegrationBase.getConsumerProperties(bootstrapServers,
                ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        final Map<String, Object> offsetRecs = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList("connect-offset-topic-" + CONNECTOR_NAME));
            await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                offsetRecs.putAll(IntegrationBase.consumeOffsetMessages(consumer));
                assertThat(offsetRecs).containsExactlyInAnyOrderEntriesOf(expectedRecords);
            });
        }
    }

    private void verifyOffsetsConsumeableByS3OffsetMgr(final Map<String, String> connectorConfig,
            final Collection<String> offsetKeys, final Map<String, Long> expected) {

        final OffsetBackingStore backingStore = IntegrationBase.getConnectorOffsetBackingStore(
                connectRunner.getBootstrapServers(), connectorConfig, connectRunner.getWorkerProperties(),
                CONNECTOR_NAME);
        final OffsetStorageReader reader = IntegrationBase.getOffsetReader(backingStore, CONNECTOR_NAME);

        final WorkerSourceTaskContext sourceTask = IntegrationBase.getWorkerSourceTaskContext(reader);

        final OffsetManager<S3OffsetManagerEntry> manager = new OffsetManager<>(sourceTask);
        final List<OffsetManager.OffsetManagerKey> managerKeys = offsetKeys.stream()
                .map(key -> new S3OffsetManagerEntry(TEST_BUCKET_NAME, key).getManagerKey())
                .collect(Collectors.toList());

        manager.populateOffsetManager(managerKeys);

        for (final OffsetManager.OffsetManagerKey offsetManagerKey : managerKeys) {
            final String key = (String) offsetManagerKey.getPartitionMap().get("objectKey");
            final S3OffsetManagerEntry entry = new S3OffsetManagerEntry(TEST_BUCKET_NAME, key);// NOPMD avoid
                                                                                               // instantiation in loops
            final Optional<S3OffsetManagerEntry> actualKey = manager.getEntry(offsetManagerKey, entry::fromProperties);

            if (expected.containsKey(key)) {
                assertThat(actualKey).isPresent();
                assertThat(actualKey.get().getRecordCount()).isEqualTo(expected.get(key));
                assertThat(actualKey.get().getBucket()).isEqualTo(TEST_BUCKET_NAME);
            } else {
                // unprocessed keys should be empty
                assertThat(actualKey).isEmpty();
            }
        }
    }

}
