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
import io.aiven.kafka.connect.common.config.ParquetTestingFixture;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
        if (taskId > -1) {
            setter.taskId(taskId);
        }

        TransformerFragment.setter(configData).inputFormat(inputFormat);

        FileNameFragment.setter(configData).template(FILE_PATTERN);

        return configData;
    }

    /**
     * Test the integration with the Amazon connector
     */
    //@Disabled("Primary testing")
    @Test
    void sourceRecordIteratorBytesTest() {
        final String topic = getTopic();
        final int maxTasks = 1;
        final int taskId = 0;

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<K> expectedKeys = new ArrayList<>();
        // write 2 objects to storage
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getNativeKey());

        // we don't expect the empty one.
        final List<K> offsetKeys = new ArrayList<>(expectedKeys);
        offsetKeys.add(write(topic, new byte[0], 3).getNativeKey());

        assertThat(getNativeStorage()).hasSize(5);

        final I sourceRecordIterator = getSourceRecordIterator(createConfig(topic, taskId, maxTasks, InputFormat.BYTES), createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.BYTES));

        final HashSet<K> seenKeys = new HashSet<>();
        while (sourceRecordIterator.hasNext()) {
            final AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
            final K key = sourceRecord.getNativeKey();
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(expectedKeys);
    }

    @Disabled("Primary testing")
    @Test
    void sourceRecordIteratorAvroTest() throws IOException {
        final var topic = getTopic();
        final int maxTasks = 1;
        final int taskId = 0;

        final Map<String, String> configData = createConfig(topic, taskId, maxTasks, InputFormat.AVRO);
        KafkaFragment.setter(configData).valueConverter(AvroConverter.class);
        TransformerFragment.setter(configData).avroValueSerializer(KafkaAvroSerializer.class);

        final int numberOfRecords = 5000;

        final byte[] outputStream1 = AvroTestDataFixture.generateMockAvroData(1, numberOfRecords);
        final byte[] outputStream2 = AvroTestDataFixture.generateMockAvroData(numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream3 = AvroTestDataFixture.generateMockAvroData(2 * numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream4 = AvroTestDataFixture.generateMockAvroData(3 * numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream5 = AvroTestDataFixture.generateMockAvroData(4 * numberOfRecords + 1, numberOfRecords);

        final Set<K> offsetKeys = new HashSet<>();

        offsetKeys.add(write(topic, outputStream1, 1).getNativeKey());
        offsetKeys.add(write(topic, outputStream2, 1).getNativeKey());

        offsetKeys.add(write(topic, outputStream3, 2).getNativeKey());
        offsetKeys.add(write(topic, outputStream4, 2).getNativeKey());
        offsetKeys.add(write(topic, outputStream5, 2).getNativeKey());

        assertThat(getNativeStorage()).hasSize(5);

        final I sourceRecordIterator = getSourceRecordIterator(configData, createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.AVRO));


        final HashSet<K> seenKeys = new HashSet<>();
        final Map<K, List<Long>> seenRecords = new HashMap<>();
        while (sourceRecordIterator.hasNext()) {
            final AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
            final K key = sourceRecord.getNativeKey();
            seenRecords.compute(key, (k, v) -> {
                final List<Long> lst = v == null ? new ArrayList<>() : v; // NOPMD new object inside loop
                lst.add(sourceRecord.getOffsetManagerEntry().getRecordCount());
                return lst;
            });
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(offsetKeys);
        assertThat(seenRecords).hasSize(5);
        final List<Long> expected = new ArrayList<>();
        for (long l = 0; l < numberOfRecords; l++) {
            expected.add(l + 1);
        }
        for (final K key : offsetKeys) {
            final List<Long> seen = seenRecords.get(key);
            assertThat(seen).as("Count for " + key).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Disabled("Primary testing")
    @Test
    void verifyIteratorRehydration() {
        // create 2 files.
        final var topic = getTopic();
        final Map<String, String> configData = createConfig(topic, 0, 1, InputFormat.BYTES);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final List<K> expectedKeys = new ArrayList<>();

        final List<K> actualKeys = new ArrayList<>();

        // write 2 objects to s3
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8),0).getNativeKey());

        assertThat(getNativeStorage()).hasSize(2);


        final I sourceRecordIterator = getSourceRecordIterator(configData, createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.BYTES));
        assertThat(sourceRecordIterator).hasNext();
        AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).hasNext();
        sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).isExhausted();
        // ensure that the reload does not replay old data.
        assertThat(sourceRecordIterator).as("Reloading leads to extra entries").isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

        // write 3rd object to s3
        expectedKeys.add(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        assertThat(getNativeStorage()).hasSize(3);

        assertThat(sourceRecordIterator).hasNext();
        sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

    }

    //@Disabled("Primary testing")
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void bytesTest(boolean addPrefixFlg) throws IOException, ExecutionException, InterruptedException {
        final boolean addPrefix = true; // addPrefixFlg
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";
        final String localPrefix = addPrefix ? format("topics/%s/partition=%s/", topic, partitionId) : null;

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects to s3 test non padded partitions
        WriteResult writeResult = write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0, localPrefix);
        expectedOffsetRecords.put(writeResult.getOffsetManagerKey(), 1L);

        writeResult = write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0, localPrefix);
        expectedOffsetRecords.put(writeResult.getOffsetManagerKey(), 1L);

        writeResult = write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1, localPrefix);
        expectedOffsetRecords.put(writeResult.getOffsetManagerKey(), 1L);

        writeResult = write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1, localPrefix);
        expectedOffsetRecords.put(writeResult.getOffsetManagerKey(), 1L);

        final List<OffsetManager.OffsetManagerKey> offsetKeys = new ArrayList<>(expectedOffsetRecords.keySet());
        offsetKeys.add(write(topic, new byte[0], 3).getOffsetManagerKey());

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

        try {
            KafkaManager kafkaManager = setupKafka(true, connectorConfig);
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(5);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));


            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

//            // Verify offset positions
            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
//
//
//            // add keys we haven't processed before
            offsetKeys.add(createOffsetManagerKey(createKey("", "not-seen-topic", 1)));
            offsetKeys.add(createOffsetManagerKey(createKey("topic-one", "not-seen-topic", 1)));
            verifyOffsetsConsumeableByOffsetManager(connectorConfig, offsetKeys, expectedOffsetRecords);
        } catch (Exception e) {
            fail(e.getMessage(), e);
        } finally {
            deleteConnector();
        }
    }

    @Disabled("Primary testing")
    @ParameterizedTest
    @CsvSource({ "4096", "3000", "4101" })
    void bytesBufferTest(final int maxBufferSize) {
        final String topic = getTopic();

        final int byteArraySize = 6000;
        final byte[] testData1 = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData1[i] = ((Integer) i).byteValue();
        }
        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();

        expectedOffsetRecords.put(write(topic, testData1, 0).getOffsetManagerKey(), 2L);

        assertThat(getNativeStorage()).hasSize(1);

        final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
        KafkaFragment.setter(connectorConfig)
                .name(getConnectorName())
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class);
        SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

        TransformerFragment.setter(connectorConfig).maxBufferSize(maxBufferSize);
        getKafkaManager().configureConnector(getConnectorName(), connectorConfig);
        try {
            // Poll messages from the Kafka topic and verify the consumed data
            final List<byte[]> records = messageConsumer().consumeRawByteMessages(topic, 2, Duration.ofSeconds(60));

            assertThat(records.get(0)).hasSize(maxBufferSize);
            assertThat(records.get(1)).hasSize(byteArraySize - maxBufferSize);

            assertThat(records.get(0)).isEqualTo(Arrays.copyOfRange(testData1, 0, maxBufferSize));
            assertThat(records.get(1)).isEqualTo(Arrays.copyOfRange(testData1, maxBufferSize, testData1.length));

            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
        } finally {
            getKafkaManager().deleteConnector(getConnectorName());
        }
    }

    @Disabled("Primary testing")
    @Test
    void avroTest(final TestInfo testInfo) throws IOException {
        final var topic = getTopic();
//        final boolean addPrefix = false;
//        final Map<String, String> connectorConfig = getAvroConfig(topic, InputFormat.AVRO, addPrefix, "", "",
//                DistributionType.OBJECT_HASH);

        /*
          final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 4, distributionType, addPrefix,
                s3Prefix, prefixPattern, "-");

        connectorConfig.put(INPUT_FORMAT_KEY, inputFormat.getValue());
        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(AVRO_VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        return connectorConfig;
         */

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

        final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.AVRO);
        KafkaFragment.setter(connectorConfig)
                .name(getConnectorName())
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(AvroConverter.class);

        TransformerFragment.setter(connectorConfig).valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                .schemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                .avroValueSerializer(KafkaAvroSerializer.class);

        SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.OBJECT_HASH);

        getKafkaManager().configureConnector(getConnectorName(), connectorConfig);
        try {

            // Poll Avro messages from the Kafka topic and deserialize them
            // Waiting for 25k kafka records in this test so a longer Duration is added.
            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, numOfRecsFactor * 5, getKafkaManager().getSchemaRegistryUrl(), Duration.ofMinutes(3));
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

//        final Map<String, Long> expectedRecords = offsetKeys.stream()
//                .collect(Collectors.toMap(Function.identity(), s -> (long) numOfRecsFactor));
//        verifyOffsetPositions(expectedRecords, connectRunner.getBootstrapServers());
            verifyOffsetPositions(expectedOffsetRecords, Duration.ofSeconds(120));
//        verifyOffsetsConsumeableByOffsetManager(connectorConfig, offsetKeys, expectedRecords);
            verifyOffsetsConsumeableByOffsetManager(connectorConfig, expectedOffsetRecords.keySet(), expectedOffsetRecords);
        } finally {
            getKafkaManager().deleteConnector(getConnectorName());
        }
    }

    @Disabled("Primary testing")
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void parquetTest(final boolean addPrefix) throws IOException {
        final var topic = getTopic();
        final int partition = 0;
        final String prefixPattern = "bucket/topics/{{topic}}/partition/{{partition}}/";
        String prefix = addPrefix ? format("topics/%s/partition=%s/", topic, partition) : null;
        final String name = "testuser";

        // write the avro messages
        final K objectKey = createKey(prefix, topic, partition);
        //final Path parquetFileDir = Files.createTempDirectory("parquet_tests");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ParquetTestingFixture.writeParquetFile(() -> baos, name, 100);
            baos.flush();
            writeWithKey(objectKey, baos.toByteArray());
        }

//        String localS3Prefix = "";
//        if (addPrefix) {
//            localS3Prefix = "bucket/topics/" + topic + "/partition/" + partition + "/";
//        }

//        final String fileName = prefix + topic + "-" + partition + "-" + System.currentTimeMillis() + ".txt";



//        final Map<String, String> connectorConfig = getAvroConfig(topic, InputFormat.PARQUET, addPrefix, localS3Prefix,
//                prefixPattern, DistributionType.PARTITION);

        final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 4, InputFormat.AVRO);
        KafkaFragment.setter(connectorConfig)
                .name(getConnectorName())
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(AvroConverter.class);

        TransformerFragment.setter(connectorConfig).valueConverterSchemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                .schemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                .avroValueSerializer(KafkaAvroSerializer.class);

        SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

        if (addPrefix) {
            FileNameFragment.setter(connectorConfig).prefixTemplate(prefixPattern);
        }

        getKafkaManager().configureConnector(getConnectorName(), connectorConfig);
        try {


//
//        final Path parquetFileDir = tempDir.resolve("parquet_tests/user.parquet");
//        Files.createDirectories(parquetFileDir);
//        final Path path = ParquetTestingFixture.writeParquetFile(parquetFileDir, name, 100);
//
//        write(topic, )
//        try {
//            s3Client.putObject(PutObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(fileName).build(), path);
//        } catch (final Exception e) { // NOPMD broad exception caught
//            LOGGER.error("Error in reading file {}", e.getMessage(), e);
//        } finally {
//            Files.delete(path);
//        }

            // Waiting for a small number of messages so using a smaller Duration of a minute
            final List<GenericRecord> records = messageConsumer().consumeAvroMessages(topic, 100, getKafkaManager().getSchemaRegistryUrl(), Duration.ofSeconds(60));
            final List<String> expectedRecordNames = IntStream.range(0, 100)
                    .mapToObj(i -> name + i)
                    .collect(Collectors.toList());
            assertThat(records).extracting(record -> record.get("name").toString())
                    .containsExactlyInAnyOrderElementsOf(expectedRecordNames);
        } finally {
            getKafkaManager().deleteConnector(getConnectorName());
        }
    }

//    private Map<String, String> getAvroConfig(final String topic, final InputFormat inputFormat,
//                                              final boolean addPrefix, final String s3Prefix, final String prefixPattern,
//                                              final DistributionType distributionType) {
//        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 4, distributionType, addPrefix,
//                s3Prefix, prefixPattern, "-");
//
//        connectorConfig.put(INPUT_FORMAT_KEY, inputFormat.getValue());
//        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
//        connectorConfig.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
//        connectorConfig.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
//        connectorConfig.put(AVRO_VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        return connectorConfig;
//    }

    @Disabled("Primary testing")
    @Test
    void jsonTest() throws IOException, ExecutionException, InterruptedException {

        final var topic = getTopic();

//        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topic, 1, DistributionType.PARTITION,
//                false, "", "", "-");
//        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.JSONL.getValue());
//        connectorConfig.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.json.JsonConverter");


        final String testMessage = "This is a test ";
        final byte[] jsonBytes = JsonTestDataFixture.getJsonRecs(500, testMessage).getBytes(StandardCharsets.UTF_8);
        final WriteResult writeResult = write(topic, jsonBytes, 1);


        final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.JSONL);
        KafkaFragment.setter(connectorConfig)
                .name(getConnectorName())
                .valueConverter(JsonConverter.class);
        SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

        setupKafka(connectorConfig).configureConnector(getConnectorName(), connectorConfig);
        try {
            getKafkaManager().createTopic(topic);
            // Poll Json messages from the Kafka topic and deserialize them
            final List<JsonNode> records = messageConsumer().consumeJsonMessages(topic, 500, Duration.ofSeconds(60));

            assertThat(records).map(jsonNode -> jsonNode.get("payload")).anySatisfy(jsonNode -> {
                assertThat(jsonNode.get("message").asText()).contains(testMessage);
                assertThat(jsonNode.get("id").asText()).contains("1");
            });

            // Verify offset positions
//        final Map<String, Long> expectedRecords = Map.of(offsetKey, 500L);
//        verifyOffsetPositions(expectedRecords, connectRunner.getBootstrapServers());
//        // Fake key 1 and 2 will not be populated into the offset manager.
//        verifyOffsetsConsumeableByOffsetManager(connectorConfig, List.of(offsetKey, "fake-key-1", "fake-key-2"),
//                expectedRecords);
        } finally {
            getKafkaManager().deleteConnector(getConnectorName());
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
                assertThat(offsetRecs).containsExactlyInAnyOrderEntriesOf(expectedRecords);
            });
        }
    }

    private void verifyOffsetsConsumeableByOffsetManager(final Map<String, String> connectorConfig,
                                                         final Collection<OffsetManager.OffsetManagerKey> offsetKeys, final Map<OffsetManager.OffsetManagerKey, Long> expectedKeys) {
        final OffsetManager<O> manager = createOffsetManager(connectorConfig);
        manager.populateOffsetManager(offsetKeys);

        for (final OffsetManager.OffsetManagerKey offsetManagerKey : offsetKeys) {
            final Optional<O> actualEntry = manager.getEntry(offsetManagerKey, getOffsetManagerEntryCreator(offsetManagerKey));

            if (expectedKeys.containsKey(offsetManagerKey)) {
                assertThat(actualEntry).isPresent();
                assertThat(actualEntry.get().getRecordCount()).isEqualTo(expectedKeys.get(offsetManagerKey));
                assertThat(actualEntry.get().getManagerKey()).isEqualTo(offsetManagerKey);
            } else {
                // unprocessed keys should be empty
                assertThat(actualEntry).isEmpty();
            }
        }
    }

}
