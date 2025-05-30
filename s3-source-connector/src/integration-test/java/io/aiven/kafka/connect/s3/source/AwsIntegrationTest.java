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

package io.aiven.kafka.connect.s3.source;

import static io.aiven.kafka.connect.common.config.CommonConfig.MAX_TASKS;
import static io.aiven.kafka.connect.common.config.CommonConfig.TASK_ID;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC;
import static io.aiven.kafka.connect.common.config.TransformerFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_FETCH_BUFFER_SIZE;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecordIterator;

import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers
@SuppressWarnings("PMD.ExcessiveImports")
class AwsIntegrationTest implements IntegrationBase {

    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-AWS-test-";

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();

    private static String s3Prefix;

    private S3Client s3Client;
    private String s3Endpoint;

    private BucketAccessor testBucketAccessor;

    @Override
    public String getS3Prefix() {
        return s3Prefix;
    }

    @Override
    public S3Client getS3Client() {
        return s3Client;
    }

    @BeforeAll
    static void setUpAll() {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
    }

    @BeforeEach
    void setupAWS() {
        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    @AfterEach
    void tearDownAWS() {
        testBucketAccessor.removeBucket();
        s3Client.close();
    }

    private Map<String, String> getConfig(final String topic, final int maxTasks) {
        final Map<String, String> config = new HashMap<>();
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, getS3Prefix());
        config.put(TARGET_TOPIC, topic);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(MAX_TASKS, String.valueOf(maxTasks));
        config.put(AWS_S3_FETCH_BUFFER_SIZE, "2");
        return config;
    }

    /**
     * Test the integration with the Amazon connector
     *
     * @param testInfo
     *            The testing configuration.
     */
    @Test
    void sourceRecordIteratorBytesTest(final TestInfo testInfo) {
        final var topic = IntegrationBase.getTopic(testInfo);
        final int maxTasks = 1;
        final int taskId = 0;
        final Map<String, String> configData = getConfig(topic, maxTasks);

        configData.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        FileNameFragment.setter(configData).template("{{topic}}-{{partition}}-{{start_offset}}");
        configData.put(TASK_ID, String.valueOf(taskId));
        configData.put(MAX_TASKS, String.valueOf(maxTasks));
        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<String> offsetKeys = new ArrayList<>();
        final List<String> expectedKeys = new ArrayList<>();
        // write 2 objects to s3
        expectedKeys.add(writeToS3(topic, testData1.getBytes(StandardCharsets.UTF_8), "00000", s3Prefix));
        expectedKeys.add(writeToS3(topic, testData2.getBytes(StandardCharsets.UTF_8), "00000", s3Prefix));
        expectedKeys.add(writeToS3(topic, testData1.getBytes(StandardCharsets.UTF_8), "00001", s3Prefix));
        expectedKeys.add(writeToS3(topic, testData2.getBytes(StandardCharsets.UTF_8), "00001", s3Prefix));

        // we don't expext the empty one.
        offsetKeys.addAll(expectedKeys);
        offsetKeys.add(writeToS3(topic, new byte[0], "00003"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());

        final OffsetManager<S3OffsetManagerEntry> offsetManager = new OffsetManager<>(context);

        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig);

        final Iterator<S3SourceRecord> sourceRecordIterator = new S3SourceRecordIterator(s3SourceConfig, offsetManager,
                TransformerFactory.getTransformer(InputFormat.BYTES), sourceClient);

        final HashSet<String> seenKeys = new HashSet<>();
        while (sourceRecordIterator.hasNext()) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            final String key = s3SourceRecord.getNativeKey();
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(expectedKeys);
    }

    @Test
    void sourceRecordIteratorAvroTest(final TestInfo testInfo) throws IOException {
        final var topic = IntegrationBase.getTopic(testInfo);
        final int maxTasks = 1;
        final int taskId = 0;

        final Map<String, String> configData = getConfig(topic, maxTasks);

        configData.put(INPUT_FORMAT_KEY, InputFormat.AVRO.getValue());
        configData.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
        configData.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        FileNameFragment.setter(configData).template("{{topic}}-{{partition}}-{{start_offset}}");
        configData.put(TASK_ID, String.valueOf(taskId));
        configData.put(MAX_TASKS, String.valueOf(maxTasks));

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

        offsetKeys.add(writeToS3(topic, outputStream1, "00001", s3Prefix));
        offsetKeys.add(writeToS3(topic, outputStream2, "00001", s3Prefix));

        offsetKeys.add(writeToS3(topic, outputStream3, "00002", s3Prefix));
        offsetKeys.add(writeToS3(topic, outputStream4, "00002", s3Prefix));
        offsetKeys.add(writeToS3(topic, outputStream5, "00002", s3Prefix));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());

        final OffsetManager<S3OffsetManagerEntry> offsetManager = new OffsetManager(context);

        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig);

        final Iterator<S3SourceRecord> sourceRecordIterator = new S3SourceRecordIterator(s3SourceConfig, offsetManager,
                TransformerFactory.getTransformer(InputFormat.AVRO), sourceClient);

        final HashSet<String> seenKeys = new HashSet<>();
        final Map<String, List<Long>> seenRecords = new HashMap<>();
        while (sourceRecordIterator.hasNext()) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            final String key = s3SourceRecord.getNativeKey();
            seenRecords.compute(key, (k, v) -> {
                final List<Long> lst = v == null ? new ArrayList<>() : v; // NOPMD new object inside loop
                lst.add(s3SourceRecord.getOffsetManagerEntry().getRecordCount());
                return lst;
            });
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(offsetKeys);
        assertThat(seenRecords).hasSize(5);
        final List<Long> expected = new ArrayList<>();
        for (long l = 0; l < numOfRecsFactor; l++) {
            expected.add(l + 1);
        }
        for (final String key : offsetKeys) {
            final List<Long> seen = seenRecords.get(key);
            assertThat(seen).as("Count for " + key).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void verifyIteratorRehydration(final TestInfo testInfo) {
        // create 2 files.
        final var topic = IntegrationBase.getTopic(testInfo);
        final Map<String, String> configData = getConfig(topic, 1);
        configData.put(TASK_ID, "0");

        configData.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        FileNameFragment.setter(configData).template("{{topic}}-{{partition}}");

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final List<String> expectedKeys = new ArrayList<>();

        final List<String> actualKeys = new ArrayList<>();

        // write 2 objects to s3
        expectedKeys.add(writeToS3(topic, testData1.getBytes(StandardCharsets.UTF_8), "00000", s3Prefix));
        expectedKeys.add(writeToS3(topic, testData2.getBytes(StandardCharsets.UTF_8), "00000", s3Prefix));

        assertThat(testBucketAccessor.listObjects()).hasSize(2);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig);
        final Iterator<S3SourceRecord> iterator = new S3SourceRecordIterator(s3SourceConfig, createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.BYTES), sourceClient);

        assertThat(iterator).hasNext();
        S3SourceRecord s3SourceRecord = iterator.next();
        actualKeys.add(s3SourceRecord.getNativeKey());
        assertThat(iterator).hasNext();
        s3SourceRecord = iterator.next();
        actualKeys.add(s3SourceRecord.getNativeKey());
        assertThat(iterator).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

        // write 3rd object to s3
        expectedKeys.add(writeToS3(topic, testData3.getBytes(StandardCharsets.UTF_8), "00000", s3Prefix));
        assertThat(testBucketAccessor.listObjects()).hasSize(3);

        assertThat(iterator).hasNext();
        s3SourceRecord = iterator.next();
        actualKeys.add(s3SourceRecord.getNativeKey());
        assertThat(iterator).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

    }

    private static @NotNull OffsetManager<S3OffsetManagerEntry> createOffsetManager() {
        return new OffsetManager<>(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Map.of();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(final Map<String, T> map) {
                        return Map.of();
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            final Collection<Map<String, T>> collection) {
                        return Map.of();
                    }
                };
            }
        });
    }
}
