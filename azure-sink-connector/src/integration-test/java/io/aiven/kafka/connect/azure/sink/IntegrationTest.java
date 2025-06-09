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

package io.aiven.kafka.connect.azure.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class IntegrationTest extends AbstractIntegrationTest<byte[], byte[]> {
    private static final String CONNECTOR_NAME = "aiven-azure-sink-connector";

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBlobAccessor.clear(azurePrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaManager().bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        super.startConnectRunner(producerProps);
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void basicTest(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 1000; i++) {
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
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            blobContents.put(blobName,
                    testBlobAccessor.readAndDecodeLines(blobName, compression, 0, 1)
                            .stream()
                            .map(fields -> String.join(",", fields))
                            .collect(Collectors.toList()));
        }

        cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                final String blobName = getBlobName(partition, 0, compression);
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = key + "," + value;
                assertThat(actualLine).isEqualTo(expectedLine);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void groupByTimestampVariable(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-0".getBytes(StandardCharsets.UTF_8),
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 1, "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 3, "key-4".getBytes(StandardCharsets.UTF_8),
                "value-4".getBytes(StandardCharsets.UTF_8)));

        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final Map<String, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getTimestampBlobName(0, 0),
                new String[] { "key-0,value-0", "key-1,value-1", "key-2,value-2" });
        expectedBlobsAndContent.put(getTimestampBlobName(1, 0), new String[] { "key-3,value-3" });
        expectedBlobsAndContent.put(getTimestampBlobName(3, 0), new String[] { "key-4,value-4" });

        final List<String> expectedBlobsNames = expectedBlobsAndContent.keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        awaitAllBlobsWritten(expectedBlobsNames.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobsNames);

        for (final String expectedBlobName : expectedBlobsNames) {
            final List<String> blobContent = testBlobAccessor.readAndDecodeLines(expectedBlobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields).trim())
                    .collect(Collectors.toList());

            assertThat(blobContent).containsExactlyInAnyOrder(expectedBlobsAndContent.get(expectedBlobName));
        }
    }

    private String getTimestampBlobName(final int partition, final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s-%s-%s", azurePrefix, testTopic0, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")), time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void oneFilePerRecordWithPlainValues(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "value");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        connectorConfig.put(AzureBlobSinkConfig.FILE_MAX_RECORDS, "1");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-0".getBytes(StandardCharsets.UTF_8),
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 0, "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 1, "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic0, 3, "key-4".getBytes(StandardCharsets.UTF_8),
                "value-4".getBytes(StandardCharsets.UTF_8)));

        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getBlobName(0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getBlobName(0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getBlobName(0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getBlobName(1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getBlobName(3, 0, compression), "value-4");
        final List<String> expectedBlobsNames = expectedBlobsAndContent.keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        awaitAllBlobsWritten(expectedBlobsNames.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobsNames);

        for (final Map.Entry<String, String> entry : expectedBlobsAndContent.entrySet()) {
            assertThat(testBlobAccessor.readStringContent(entry.getKey(), compression))
                    .isEqualTo(expectedBlobsAndContent.get(entry.getKey()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void groupByKey(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final CompressionType compressionType = CompressionType.forName(compression);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{key}}" + compressionType.extension());
        createConnector(connectorConfig);

        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(new TopicPartition(testTopic0, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3"));
        keysPerTopicPartition.put(new TopicPartition(testTopic0, 1), Arrays.asList("key-4", "key-5", "key-6"));
        keysPerTopicPartition.put(new TopicPartition(testTopic0, 0), Arrays.asList(null, "key-7"));
        keysPerTopicPartition.put(new TopicPartition(testTopic0, 1), Arrays.asList("key-8"));

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<String, String> lastValuePerKey = new HashMap<>();
        final int cntMax = 1000;
        int cnt = 0;
        outer : while (true) {
            for (final Map.Entry<TopicPartition, List<String>> entry : keysPerTopicPartition.entrySet()) {
                for (final String key : keysPerTopicPartition.get(entry.getKey())) {
                    final String value = "value-" + cnt;
                    cnt += 1;
                    final byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
                    sendFutures.add(sendMessageAsync(entry.getKey().topic(), entry.getKey().partition(), keyBytes,
                            value.getBytes(StandardCharsets.UTF_8)));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = keysPerTopicPartition.values()
                .stream()
                .flatMap(keys -> keys.stream().map(k -> getBlobName(k, compression)))
                .collect(Collectors.toList());

        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyInAnyOrderElementsOf(expectedBlobs);

        for (final String blobName : expectedBlobs) {
            final String blobContent = testBlobAccessor.readAndDecodeLines(blobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());
            final String keyInBlobName = blobName.replace(azurePrefix, "").replace(compressionType.extension(), "");
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
    void jsonlOutput() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String compression = "none";
        final String contentType = "jsonl";
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, contentType);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
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
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBlobAccessor.readLines(blobName, compression)); // NOPMD
            // instantiation
            // in a
            // loop
            blobContents.put(blobName, items);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                final String blobName = getBlobName(partition, 0, "none");
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";
                assertThat(actualLine).isEqualTo(expectedLine);
            }
        }
    }

    @Test
    void jsonOutput() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String compression = "none";
        final String contentType = "json";
        connectorConfig.put("azure.storage.connection.string",
                azureEndpoint != null ? azureEndpoint : azureConnectionString); // NOPMD
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, contentType);
        createConnector(connectorConfig);

        final int numEpochs = 10;

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < numEpochs; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
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
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBlobAccessor.readLines(blobName, compression)); // NOPMD
            // instantiation
            // in a
            // loop
            assertThat(items).hasSize(numEpochs + 2);
            blobContents.put(blobName, items);
        }

        // each blob should be a JSONArray
        final Map<String, List<String>> jsonContents = new HashMap<>();
        for (int partition = 0; partition < 4; partition++) {
            final String blobName = getBlobName(partition, 0, compression);
            final List<String> blobContent = blobContents.get(blobName);
            assertThat(blobContent).startsWith("[").endsWith("]");
            jsonContents.put(blobName, blobContent.subList(1, blobContent.size() - 1));
        }

        cnt = 0;
        for (int i = 0; i < numEpochs; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                final String blobName = getBlobName(partition, 0, compression);
                final String actualLine = jsonContents.get(blobName).get(i);
                String expectedLine = String.format("{\"value\":%s,\"key\":\"%s\"}", value, key);
                expectedLine = i < (jsonContents.get(blobName).size() - 1)
                        ? String.format("%s,", expectedLine)
                        : expectedLine;
                assertThat(actualLine).isEqualTo(expectedLine);
            }
        }
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put(AzureBlobSinkConfig.NAME_CONFIG, CONNECTOR_NAME);
        config.put("connector.class", AzureBlobSinkConnector.class.getName());
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONTAINER_NAME_CONFIG, testContainerName);
        if (useFakeAzure()) {
            config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, azureEndpoint);
        } else {
            config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, azureConnectionString);
        }
        config.put(AzureBlobSinkConfig.FILE_NAME_PREFIX_CONFIG, azurePrefix);
        config.put("topics", testTopic0 + "," + testTopic1);
        return config;
    }
}
