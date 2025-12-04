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

package io.aiven.kafka.connect.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class ParquetIntegrationTest extends AbstractIntegrationTest<byte[], byte[]> {

    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector-plain-parquet";

    @TempDir
    Path tmpDir;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBucketAccessor.clear(gcsPrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaManager().bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        startConnectRunner(producerProps);
    }

    @Test
    void allOutputFields() throws ExecutionException, InterruptedException, IOException {
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE, OutputFieldType.OFFSET,
                        OutputFieldType.TIMESTAMP, OutputFieldType.HEADERS)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        CommonConfigFragment.setter(connectorConfig)
                .keyConverter(StringConverter.class)
                .valueConverter(StringConverter.class);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;
                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                assertThat(record.get("key")).hasToString(key);
                assertThat(record.get("value")).hasToString(value);
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                cnt += 1;
            }
        }
    }

    @Test
    void allOutputFieldsJsonValueAsString() throws ExecutionException, InterruptedException, IOException {
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE, OutputFieldType.OFFSET,
                        OutputFieldType.TIMESTAMP, OutputFieldType.HEADERS)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        CommonConfigFragment.setter(connectorConfig)
                .keyConverter(StringConverter.class)
                .valueConverter(StringConverter.class);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                cnt += 1;
                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final var value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                assertThat(record.get("key")).hasToString(key);
                assertThat(record.get("value")).hasToString(value);
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                cnt += 1;
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "true, {\"value\": {\"name\": \"%s\"}} ", "false, {\"name\": \"%s\"}" })
    void jsonValue(final String envelopeEnabled, final String expectedOutput)
            throws ExecutionException, InterruptedException, IOException {
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.VALUE)
                .envelopeEnabled(true)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        CommonConfigFragment.setter(connectorConfig)
                .keyConverter(StringConverter.class)
                .valueConverter(JsonConverter.class);
        createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = String.format(jsonMessagePattern, jsonMessageSchema,
                        "{" + "\"name\":\"user-" + cnt + "\"}");
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                final String expectedLine = String.format(expectedOutput, name);
                assertThat(record).hasToString(expectedLine);
                cnt += 1;
            }
        }
    }

    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.VALUE)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        CommonConfigFragment.setter(connectorConfig)
                .keyConverter(StringConverter.class)
                .valueConverter(JsonConverter.class);
        createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessageNewSchema = "{\"type\":\"struct\",\"fields\":"
                + "[{\"type\":\"string\",\"field\":\"name\"}, "
                + "{\"type\":\"string\",\"field\":\"value\", \"default\": \"foo\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final var expectedRecords = new ArrayList<String>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final String value;
                final String payload;
                if (i < 5) { // NOPMD literal
                    payload = "{" + "\"name\": \"user-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageSchema, payload);
                } else {
                    payload = "{" + "\"name\": \"user-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageNewSchema, payload);
                }
                expectedRecords.add(String.format("{\"value\": %s}", payload));
                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
                cnt += 1;
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(0, 5, compression),
                getBlobName(1, 0, compression), getBlobName(1, 5, compression), getBlobName(2, 0, compression),
                getBlobName(2, 5, compression), getBlobName(3, 0, compression), getBlobName(3, 5, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);

        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.addAll(records.stream().map(GenericRecord::toString).collect(Collectors.toList()));
        }
        assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }
    private Map<String, String> basicConnectorConfig(final CompressionType compression) {
        final Map<String, String> config = new HashMap<>();
        CommonConfigFragment.setter(config)
                .name(CONNECTOR_NAME)
                .connector(GcsSinkConnector.class)
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class);
        CommonConfigFragment.setter(config).maxTasks(1);
        FileNameFragment.setter(config).fileCompression(compression).prefix(gcsPrefix);
        OutputFormatFragment.setter(config).withFormatType(FormatType.PARQUET);
        if (gcsCredentialsPath != null) {
            config.put("gcs.credentials.path", gcsCredentialsPath);
        }
        if (gcsCredentialsJson != null) {
            config.put("gcs.credentials.json", gcsCredentialsJson);
        }
        if (useFakeGCS()) {
            config.put("gcs.endpoint", gcsEndpoint);
        }
        config.put("gcs.bucket.name", testBucketName);
        config.put("topics", testTopic0 + "," + testTopic1);
        return config;
    }

}
