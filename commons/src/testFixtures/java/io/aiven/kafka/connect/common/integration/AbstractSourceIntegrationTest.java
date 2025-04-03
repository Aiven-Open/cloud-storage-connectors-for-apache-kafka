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

import com.fasterxml.jackson.databind.JsonNode;
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
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest.FILE_PATTERN;
import static java.lang.String.format;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 *
 * @param <K> the native key type.
 */
//@SuppressWarnings("PMD.ExcessiveImports")
public abstract class AbstractSourceIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>,
        I extends AbstractSourceRecordIterator<?, K, O, ?>> extends AbstractIntegrationTest<K, O, I> {

    private static final int TASK_NOT_SET = -1;

    protected Duration getOffsetFlushInterval() {
        return Duration.ofMillis(500);
    }

    protected abstract OffsetManager.OffsetManagerKey createOffsetManagerKey(K nativeKey);

    protected abstract  Function<Map<String, Object>, O> getOffsetManagerEntryCreator(OffsetManager.OffsetManagerKey key);

    protected abstract I getSourceRecordIterator(Map<String, String> configData, OffsetManager<O> offsetManager,
                                                 Transformer transformer);

    final protected OffsetManager<O> createOffsetManager() {
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());
        return new OffsetManager<>(context);
    }

    private Map<String, String> createConfig(final String topic, final int taskId, final int maxTasks, final InputFormat inputFormat) {
        return createConfig(null, topic, taskId, maxTasks, inputFormat);
    }

    private Map<String, String> createConfig(String localPrefix, final String topic, final int taskId, final int maxTasks, final InputFormat inputFormat) {
        final Map<String, String> configData = createConnectorConfig(localPrefix);

        KafkaFragment.setter(configData)
                .connector(getConnectorClass());

        SourceConfigFragment.setter(configData).targetTopic(topic);

        CommonConfigFragment.Setter setter = CommonConfigFragment.setter(configData).maxTasks(maxTasks);
        if (taskId > TASK_NOT_SET) {
            setter.taskId(taskId);
        }

        if (inputFormat == InputFormat.AVRO)
        {
            configData.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        }
        TransformerFragment.setter(configData).inputFormat(inputFormat);

        FileNameFragment.setter(configData).template(FILE_PATTERN);

        return configData;
    }

    /**
     * Verify that the offset manager can read the data and skip already read messages.
     */
    @Test
    void writeBeforeRestartReadsNewRecordsTest() {
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        final List<OffsetManager.OffsetManagerKey> offsetKeys = new ArrayList<>(expectedOffsetRecords.keySet());
        offsetKeys.add(write(topic, new byte[0], 3).getOffsetManagerKey());

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(5);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

            // write new data
            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);
            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 2).getOffsetManagerKey(), 1L);

            kafkaManager.restartConnector(getConnectorName());

            records = messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(10));
            assertThat(records).containsOnly(testData3);
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Verify that the offset manager can read the data and skip already read messages.
     */
    @Test
    void writeAfterRestartReadsNewRecordsTest() {
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        final List<OffsetManager.OffsetManagerKey> offsetKeys = new ArrayList<>(expectedOffsetRecords.keySet());
        offsetKeys.add(write(topic, new byte[0], 3).getOffsetManagerKey());

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(5);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);


            kafkaManager.restartConnector(getConnectorName());

            // write new data
            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);
            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 2).getOffsetManagerKey(), 1L);

            records = messageConsumer().consumeByteMessages(topic, 2, Duration.ofSeconds(20));
            assertThat(records).containsOnly(testData3);
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void bytesTest(boolean addPrefix) throws IOException, ExecutionException, InterruptedException {
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";
        final String localPrefix = addPrefix ? format("topics/%s/partition=%s/", topic, partitionId) : null;

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1, localPrefix).getOffsetManagerKey(), 1L);
        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1, localPrefix).getOffsetManagerKey(), 1L);
        write(topic, new byte[0], 3);

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(localPrefix, topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);
            if (addPrefix) {
                FileNameFragment.setter(connectorConfig).prefixTemplate(prefixPattern);
            }

            KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(5);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));


            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

            // Verify offset positions
            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @ParameterizedTest
    @CsvSource({ "4096", "3000", "4101" })
    void bytesBufferSizeTest(final int maxBufferSize) throws IOException, ExecutionException, InterruptedException {
        final String topic = getTopic();

        final int byteArraySize = 6000;
        final byte[] testData1 = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData1[i] = ((Integer) i).byteValue();
        }
        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();

        expectedOffsetRecords.put(write(topic, testData1, 0).getOffsetManagerKey(), 2L);

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
            KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            // Poll messages from the Kafka topic and verify the consumed data
            final List<byte[]> records = messageConsumer().consumeRawByteMessages(topic, 2, Duration.ofSeconds(60));

            assertThat(records.get(0)).hasSize(maxBufferSize);
            assertThat(records.get(1)).hasSize(byteArraySize - maxBufferSize);

            assertThat(records.get(0)).isEqualTo(Arrays.copyOfRange(testData1, 0, maxBufferSize));
            assertThat(records.get(1)).isEqualTo(Arrays.copyOfRange(testData1, maxBufferSize, testData1.length));

            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @Test
    void avroTest() throws IOException, ExecutionException, InterruptedException {
        final var topic = getTopic();

        final int numOfRecsFactor = 5000;

        final byte[] outputStream1 = AvroTestDataFixture.generateMockAvroData(1, numOfRecsFactor);
        final byte[] outputStream2 = AvroTestDataFixture.generateMockAvroData(numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream3 = AvroTestDataFixture.generateMockAvroData(2 * numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream4 = AvroTestDataFixture.generateMockAvroData(3 * numOfRecsFactor + 1, numOfRecsFactor);
        final byte[] outputStream5 = AvroTestDataFixture.generateMockAvroData(4 * numOfRecsFactor + 1, numOfRecsFactor);

        final Set<K> offsetKeys = new HashSet<>();
        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();

        // test padded partition ids
        expectedOffsetRecords.put(write(topic, outputStream1, 1).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream2, 1).getOffsetManagerKey(), (long) numOfRecsFactor);

        expectedOffsetRecords.put(write(topic, outputStream3, 2).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream4, 2).getOffsetManagerKey(), (long) numOfRecsFactor);
        expectedOffsetRecords.put(write(topic, outputStream5, 2).getOffsetManagerKey(), (long) numOfRecsFactor);

        assertThat(getNativeStorage()).hasSize(5);

        try {
            // create configuration
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.AVRO);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .valueConverter(AvroConverter.class);

            // start the manager
            KafkaManager kafkaManager = setupKafka();

            TransformerFragment.setter(connectorConfig).valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .schemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .avroValueSerializer(KafkaAvroSerializer.class);

            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.OBJECT_HASH);

            // finish configuring the manager
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            // Poll Avro messages from the Kafka topic and deserialize them
            // Waiting for 25k kafka records in this test so a longer Duration is added.
            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, numOfRecsFactor * 5, getKafkaManager().getSchemaRegistryUrl(), Duration.ofSeconds(30));
            // Ensure this method deserializes Avro

            // Verify that the correct data is read from storage and pushed to Kafka
            assertThat(records).map(record -> entry(record.get("id"), String.valueOf(record.get("message"))))
                    .contains(entry(1, "Hello, Kafka Connect S3 Source! object 1"),
                            entry(2, "Hello, Kafka Connect S3 Source! object 2"),
                            entry(numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + numOfRecsFactor),
                            entry(2 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (2 * numOfRecsFactor)),
                            entry(3 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (3 * numOfRecsFactor)),
                            entry(4 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (4 * numOfRecsFactor)),
                            entry(5 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (5 * numOfRecsFactor)));

            verifyOffsetPositions(expectedOffsetRecords, Duration.ofMinutes(1));
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void parquetTest(final boolean addPrefix) throws IOException, ExecutionException, InterruptedException {
        final var topic = getTopic();
        final int partition = 0;
        final String prefixPattern = "bucket/topics/{{topic}}/partition/{{partition}}/";
        String prefix = addPrefix ? format("topics/%s/partition=%s/", topic, partition) : null;
        final String name = "testuser";

        // write the avro messages
        final K objectKey = createKey(prefix, topic, partition);
        writeWithKey(objectKey, ParquetTestDataFixture.generateMockParquetData(name, 100));

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
            KafkaManager kafkaManager = setupKafka();

            TransformerFragment.setter(connectorConfig).valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .schemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .avroValueSerializer(KafkaAvroSerializer.class);


            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, 100, getKafkaManager().getSchemaRegistryUrl(), Duration.ofSeconds(20));
            final List<String> expectedRecordNames = IntStream.range(0, 100)
                    .mapToObj(i -> name + i)
                    .collect(Collectors.toList());
            assertThat(records).extracting(record -> record.get("name").toString())
                    .containsExactlyInAnyOrderElementsOf(expectedRecordNames);
        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    @Test
    void jsonlTest() throws IOException, ExecutionException, InterruptedException {

        final var topic = getTopic();

        final String testMessage = "This is a test ";
        final byte[] jsonBytes = JsonTestDataFixture.getJsonRecs(500, testMessage).getBytes(StandardCharsets.UTF_8);
        Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        expectedOffsetRecords.put(write(topic, jsonBytes, 1).getOffsetManagerKey(), 500L);

        try {

            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.JSONL);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .valueConverter(JsonConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            KafkaManager kafkaManager = setupKafka();
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

        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
           deleteConnector();
        }
    }

    protected void verifyOffsetPositions(final Map<OffsetManager.OffsetManagerKey, Long> expectedRecords, Duration timeLimit) {
        final Properties consumerProperties = consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class)
                .valueDeserializer(ByteArrayDeserializer.class).build();
        MessageConsumer messageConsumer = messageConsumer();
        KafkaManager kafkaManager = getKafkaManager();
        final Map<OffsetManager.OffsetManagerKey, Long> offsetRecs = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(kafkaManager.getOffsetTopic()));
            await().atMost(timeLimit).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                messageConsumer.consumeOffsetMessages(consumer).forEach( o -> offsetRecs.put(o.getManagerKey(), o.getRecordCount()));
                assertThat(offsetRecs).containsAllEntriesOf(expectedRecords);
            });
        }
    }

}
