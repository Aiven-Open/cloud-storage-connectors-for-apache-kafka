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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.AVRO_VALUE_SERIALIZER;
import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPICS;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

@Testcontainers
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

    private Map<String, String> getConfig(final String topics, final int maxTasks) {
        final Map<String, String> config = new HashMap<>();
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, getS3Prefix());
        config.put(TARGET_TOPIC_PARTITIONS, "0,1");
        config.put(TARGET_TOPICS, topics);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", String.valueOf(maxTasks));
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
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> configData = getConfig(topicName, 1);

        configData.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<String> offsetKeys = new ArrayList<>();
        final List<String> expectedKeys = new ArrayList<>();
        // write 2 objects to s3
        expectedKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00000"));
        expectedKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00000"));
        expectedKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00001"));
        expectedKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00001"));

        // we don't expext the empty one.
        offsetKeys.addAll(expectedKeys);
        offsetKeys.add(writeToS3(topicName, new byte[0], "00003"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());

        final OffsetManager<S3OffsetManagerEntry> offsetManager = new OffsetManager<>(context);

        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig, new HashSet<>());

        final Iterator<S3SourceRecord> sourceRecordIterator = new SourceRecordIterator(s3SourceConfig, offsetManager,
                TransformerFactory.getTransformer(InputFormat.BYTES), sourceClient);

        final HashSet<String> seenKeys = new HashSet<>();
        while (sourceRecordIterator.hasNext()) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            final String key = s3SourceRecord.getObjectKey();
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(expectedKeys);
    }

    @Test
    void sourceRecordIteratorAvroTest(final TestInfo testInfo) throws IOException {
        final var topicName = IntegrationBase.topicName(testInfo);

        final Map<String, String> configData = getConfig(topicName, 1);

        configData.put(INPUT_FORMAT_KEY, InputFormat.AVRO.getValue());
        configData.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
        configData.put(AVRO_VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");

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

        offsetKeys.add(writeToS3(topicName, outputStream1, "00001"));
        offsetKeys.add(writeToS3(topicName, outputStream2, "00001"));

        offsetKeys.add(writeToS3(topicName, outputStream3, "00002"));
        offsetKeys.add(writeToS3(topicName, outputStream4, "00002"));
        offsetKeys.add(writeToS3(topicName, outputStream5, "00002"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());

        final OffsetManager<S3OffsetManagerEntry> offsetManager = new OffsetManager(context);

        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig, new HashSet<>());

        final Iterator<S3SourceRecord> sourceRecordIterator = new SourceRecordIterator(s3SourceConfig, offsetManager,
                TransformerFactory.getTransformer(InputFormat.AVRO), sourceClient);

        final HashSet<String> seenKeys = new HashSet<>();
        final Map<String, List<Long>> seenRecords = new HashMap<>();
        while (sourceRecordIterator.hasNext()) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            final String key = s3SourceRecord.getObjectKey();
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
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> configData = getConfig(topicName, 1);

        configData.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final List<String> expectedKeys = new ArrayList<>();

        final List<String> actualKeys = new ArrayList<>();

        // write 2 objects to s3
        expectedKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00000"));
        expectedKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00000"));

        assertThat(testBucketAccessor.listObjects()).hasSize(2);

        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        final AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig, new HashSet<>());
        final Iterator<S3Object> iter = sourceClient.getS3ObjectIterator(null);

        assertThat(iter).hasNext();
        S3Object object = iter.next();
        actualKeys.add(object.key());
        assertThat(iter).hasNext();
        object = iter.next();
        actualKeys.add(object.key());
        assertThat(iter).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

        // write 3rd object to s3
        expectedKeys.add(writeToS3(topicName, testData3.getBytes(StandardCharsets.UTF_8), "00000"));
        assertThat(testBucketAccessor.listObjects()).hasSize(3);

        assertThat(iter).hasNext();
        object = iter.next();
        actualKeys.add(object.key());
        assertThat(iter).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

    }
}
