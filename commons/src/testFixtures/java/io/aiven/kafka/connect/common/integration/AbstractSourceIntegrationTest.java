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

package io.aiven.kafka.connect.common.integration;

import static io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest.FILE_PATTERN;
import static java.lang.String.format;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to ensure that data written to the storage layer in various formats can be correctly read by the source
 * implementation.
 *
 * @param <K>
 *            the native key type.
 */
@SuppressWarnings("PMD.ExcessiveImports")
public abstract class AbstractSourceIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, I extends AbstractSourceRecordIterator<?, K, O, ?>>
        extends
            AbstractSourceIntegrationBase<K, O, I> {

    /** static to indicate that the task has not been set */
    private static final int TASK_NOT_SET = -1;

    /** 3 standard test data string */
    private static final String TEST_DATA_1 = "Hello, Kafka Connect Abstract Source! object 1";
    private static final String TEST_DATA_2 = "Hello, Kafka Connect Abstract Source! object 2";
    private static final String TEST_DATA_3 = "Hello, Kafka Connect Abstract Source! object 3";

    @Override
    protected Duration getOffsetFlushInterval() {
        return Duration.ofMillis(500); // half a second between flushes
    }

    /**
     * Creates a configuration with the specified arguments.
     *
     * @param topic
     *            the topic to write the results to.
     * @param taskId
     *            the task ID, may be {@link #TASK_NOT_SET}.
     * @param maxTasks
     *            the maximum number of tasks for the source connector.
     * @param inputFormat
     *            the input format for the data.
     * @return a map of data values to configure the connector.
     */
    private Map<String, String> createConfig(final String topic, final int taskId, final int maxTasks,
            final InputFormat inputFormat) {
        return createConfig(null, topic, taskId, maxTasks, inputFormat);
    }

    /**
     * Creates a configuration with the specified arguments.
     *
     * @param localPrefix
     *            the string to prepend to all nnative keys.
     * @param topic
     *            the topic to write the results to.
     * @param taskId
     *            the task ID, may be {@link #TASK_NOT_SET}.
     * @param maxTasks
     *            the maximum number of tasks for the source connector.
     * @param inputFormat
     *            the input format for the data.
     * @return a map of data values to configure the connector.
     */
    private Map<String, String> createConfig(final String localPrefix, final String topic, final int taskId,
            final int maxTasks, final InputFormat inputFormat) {
        final Map<String, String> configData = createConnectorConfig(localPrefix);

        KafkaFragment.setter(configData).connector(getConnectorClass());

        SourceConfigFragment.setter(configData).targetTopic(topic);

        final CommonConfigFragment.Setter setter = CommonConfigFragment.setter(configData).maxTasks(maxTasks);
        if (taskId > TASK_NOT_SET) {
            setter.taskId(taskId);
        }

        if (inputFormat == InputFormat.AVRO) {
            configData.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        }
        TransformerFragment.setter(configData).inputFormat(inputFormat);

        FileNameFragment.setter(configData).template(FILE_PATTERN);

        return configData;
    }

    /**
     * Verify that the offset manager can read the data and skip already read messages. This tests verifies that data
     * written before a restart but not read are read after the restart.
     */
    @Test
    void writeBeforeRestartReadsNewRecordsTest() {
        final String topic = getTopic();

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(4);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

            // write new data
            expectedOffsetRecords
                    .put(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);
            expectedOffsetRecords
                    .put(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 2).getOffsetManagerKey(), 1L);

            // restart the connector.
            kafkaManager.restartConnector(getConnectorName());

            // connector should skip the records that were previously read.
            records = messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(10));
            assertThat(records).containsOnly(TEST_DATA_3);
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Verify that the offset manager can read the data and skip already read messages. This tests verifies that data
     * written after restart are read on a subsequent read but that earlier data is not.
     */
    @Test
    void writeAfterRestartReadsNewRecordsTest() {
        final String topic = getTopic();

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(4);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

            // restart the connector
            kafkaManager.restartConnector(getConnectorName());

            // write new data
            expectedOffsetRecords
                    .put(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);
            expectedOffsetRecords
                    .put(write(topic, TEST_DATA_3.getBytes(StandardCharsets.UTF_8), 2).getOffsetManagerKey(), 1L);

            // verify only new records are read.
            records = messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(20));
            assertThat(records).containsOnly(TEST_DATA_3);
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @ParameterizedTest
    @EnumSource(InputFormat.class)
    void zeroLengthFilesAreIgnoredTest(final InputFormat inputFormat) {
        final String topic = getTopic();

        // write empty file object
        write(topic, new byte[0], 3);
        assertThat(getNativeStorage()).hasSize(1);

        final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(ByteArrayDeserializer.class)
                .keyDeserializer(ByteArrayDeserializer.class)
                .build();
        try {
            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);

            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, inputFormat);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            // Poll messages from the Kafka topic and verify the consumed data
            final List<ConsumerRecord<Object, Object>> records = messageConsumer().consumeMessages(topic,
                    consumerProperties, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).isEmpty();
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Tests that items written in BYTE format, with or without a prefix are correctly read and reported.
     *
     * @param addPrefix
     *            if {@code true} a prefix is added to the native key.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void bytesTest(final boolean addPrefix) {
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";
        final String localPrefix = addPrefix ? format("topics/%s/partition=%s/", topic, partitionId) : null;

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 4 objects
        expectedOffsetRecords.put(
                write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 0, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(
                write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 0, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(
                write(topic, TEST_DATA_1.getBytes(StandardCharsets.UTF_8), 1, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(
                write(topic, TEST_DATA_2.getBytes(StandardCharsets.UTF_8), 1, localPrefix).getOffsetManagerKey(), 1L);

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(localPrefix, topic, TASK_NOT_SET, 1,
                    InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);
            if (addPrefix) {
                FileNameFragment.setter(connectorConfig).prefixTemplate(prefixPattern);
            }

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(4);

            // Poll messages from the Kafka topic and verify the consumed data
            final List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(TEST_DATA_1, TEST_DATA_2);

            // Verify offset positions
            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Tests that various buffer sizes byte BYTE input split the buffer properly.
     *
     * @param maxBufferSize
     *            the buffersize to use.
     */
    @ParameterizedTest
    @CsvSource({ "4096", "3000", "4101" })
    void bytesBufferSizeTest(final int maxBufferSize) {
        final String topic = getTopic();

        final int byteArraySize = 6000;
        final byte[] testData = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData[i] = ((Integer) i).byteValue();
        }
        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();

        expectedOffsetRecords.put(write(topic, testData, 0).getOffsetManagerKey(), 2L);

        assertThat(getNativeStorage()).hasSize(1);

        try {
            // configure the consumer
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);
            TransformerFragment.setter(connectorConfig).maxBufferSize(maxBufferSize);

            // configure/start Kafka
            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            // Poll messages from the Kafka topic and verify the consumed data
            final List<byte[]> records = messageConsumer().consumeRawByteMessages(topic, 2, Duration.ofSeconds(60));

            assertThat(records.get(0)).hasSize(maxBufferSize);
            assertThat(records.get(1)).hasSize(byteArraySize - maxBufferSize);

            assertThat(records.get(0)).isEqualTo(Arrays.copyOfRange(testData, 0, maxBufferSize));
            assertThat(records.get(1)).isEqualTo(Arrays.copyOfRange(testData, maxBufferSize, testData.length));

            verifyOffsetPositions(expectedOffsetRecords, Duration.ofMinutes(2));
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Tests that items written in AVRO format are correctly read and reported.
     *
     * @throws IOException
     *             if data can not be created.
     */
    @Test
    void avroTest() throws IOException {
        final var topic = getTopic();

        final int numOfRecsFactor = 5000;

        final byte[] outputStream1 = AvroTestDataFixture.generateAvroData(1, numOfRecsFactor);
        final byte[] outputStream2 = AvroTestDataFixture.generateAvroData(numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream3 = AvroTestDataFixture.generateAvroData(2 * numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream4 = AvroTestDataFixture.generateAvroData(3 * numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream5 = AvroTestDataFixture.generateAvroData(4 * numOfRecsFactor + 1, numOfRecsFactor);

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();

        expectedOffsetRecords.put(write(topic, outputStream1, 1).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream2, 1).getOffsetManagerKey(), (long) numOfRecsFactor);

        expectedOffsetRecords.put(write(topic, outputStream3, 2).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream4, 2).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream5, 2).getOffsetManagerKey(), (long) numOfRecsFactor);

        assertThat(getNativeStorage()).hasSize(5);

        try {
            // create configuration
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.AVRO);
            KafkaFragment.setter(connectorConfig).name(getConnectorName()).valueConverter(AvroConverter.class);

            // start the manager
            final KafkaManager kafkaManager = setupKafka();

            TransformerFragment.setter(connectorConfig)
                    .valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .schemaRegistry(getKafkaManager().getSchemaRegistryUrl());

            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.OBJECT_HASH);

            // finish configuring the manager
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            // Poll Avro messages from the Kafka topic and deserialize them
            // Waiting for 25k kafka records in this test so a longer Duration is added.
            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, numOfRecsFactor * 5,
                    Duration.ofSeconds(30));
            // Ensure this method deserializes Avro

            // Verify that the correct data is read from storage and pushed to Kafka
            assertThat(records).map(record -> entry(record.get("id"), record.get("message").toString()))
                    .contains(entry(1, "Hello, from Avro Test Data Fixture! object 1"),
                            entry(2, "Hello, from Avro Test Data Fixture! object 2"),
                            entry(numOfRecsFactor, "Hello, from Avro Test Data Fixture! object " + numOfRecsFactor),
                            entry(2 * numOfRecsFactor,
                                    "Hello, from Avro Test Data Fixture! object " + (2 * numOfRecsFactor)),
                            entry(3 * numOfRecsFactor,
                                    "Hello, from Avro Test Data Fixture! object " + (3 * numOfRecsFactor)),
                            entry(4 * numOfRecsFactor,
                                    "Hello, from Avro Test Data Fixture! object " + (4 * numOfRecsFactor)),
                            entry(5 * numOfRecsFactor,
                                    "Hello, from Avro Test Data Fixture! object " + (5 * numOfRecsFactor)));

            verifyOffsetPositions(expectedOffsetRecords, Duration.ofMinutes(1));
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Tests that items written in PARQUET format, with or without a prefix are correctly read and reported.
     *
     * @param addPrefix
     *            if {@code true} a prefix is added to the native key.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void parquetTest(final boolean addPrefix) throws IOException, ExecutionException, InterruptedException {
        final var topic = getTopic();
        final int partition = 0;
        final String prefixPattern = "bucket/topics/{{topic}}/partition/{{partition}}/";
        final String prefix = addPrefix ? format("topics/%s/partition=%s/", topic, partition) : null;
        final String name = "testuser";

        // write the avro messages
        final K objectKey = createKey(prefix, topic, partition);
        writeWithKey(objectKey, ParquetTestDataFixture.generateParquetData(name, 100));

        assertThat(getNativeStorage()).hasSize(1);

        final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.PARQUET);
        KafkaFragment.setter(connectorConfig)
                .name(getConnectorName())
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(AvroConverter.class);

        SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

        if (addPrefix) {
            FileNameFragment.setter(connectorConfig).prefixTemplate(prefixPattern);
        }

        try {
            final KafkaManager kafkaManager = setupKafka();

            TransformerFragment.setter(connectorConfig)
                    .valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .schemaRegistry(getKafkaManager().getSchemaRegistryUrl());

            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, 100,
                    Duration.ofSeconds(20));
            final List<String> expectedRecordNames = IntStream.range(0, 100)
                    .mapToObj(i -> name + i)
                    .collect(Collectors.toList());
            assertThat(records).extracting(record -> record.get("name").toString())
                    .containsExactlyInAnyOrderElementsOf(expectedRecordNames);
        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Tests that items written in JSONL format are correctly read and reported.
     */
    @Test
    void jsonlTest() {

        final var topic = getTopic();

        final String testMessage = "This is a test ";
        final byte[] jsonBytes = JsonTestDataFixture.generateJsonRecs(500, testMessage)
                .getBytes(StandardCharsets.UTF_8);
        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        expectedOffsetRecords.put(write(topic, jsonBytes, 1).getOffsetManagerKey(), 500L);

        try {

            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.JSONL);
            KafkaFragment.setter(connectorConfig).name(getConnectorName()).valueConverter(JsonConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);
            // Poll Json messages from the Kafka topic and deserialize them
            final List<JsonNode> records = messageConsumer().consumeJsonMessages(topic, 500, Duration.ofSeconds(60));

            assertThat(records).map(jsonNode -> jsonNode.get("payload")).anySatisfy(jsonNode -> {
                assertThat(jsonNode.get("message").asText()).contains(testMessage);
                assertThat(jsonNode.get("id").asText()).contains("1");
            });

            // Verify offset positions
            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(5));

        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Verifies the offset positions reported on the offset topic match the expected values.
     *
     * @param expectedRecords
     *            A map of OffsetManagerKey to count.
     * @param timeLimit
     *            the maximum time to wait for the results before failing.
     */
    protected void verifyOffsetPositions(final Map<OffsetManager.OffsetManagerKey, Long> expectedRecords,
            final Duration timeLimit) {
        final Properties consumerProperties = consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class)
                .valueDeserializer(ByteArrayDeserializer.class)
                .build();
        final MessageConsumer messageConsumer = messageConsumer();
        final KafkaManager kafkaManager = getKafkaManager();
        final Map<OffsetManager.OffsetManagerKey, Long> offsetRecs = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(kafkaManager.getOffsetTopic()));
            await().atMost(timeLimit).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                messageConsumer.consumeOffsetMessages(consumer)
                        .forEach(o -> offsetRecs.put(o.getManagerKey(), o.getRecordCount()));
                assertThat(offsetRecs).containsAllEntriesOf(expectedRecords);
            });
        }
    }
}
