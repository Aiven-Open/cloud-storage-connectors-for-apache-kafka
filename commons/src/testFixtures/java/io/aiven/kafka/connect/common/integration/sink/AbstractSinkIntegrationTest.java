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

package io.aiven.kafka.connect.common.integration.sink;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSinkIntegrationTest<K extends Comparable<K>, N> extends AbstractSinkIntegrationBase<K, N, byte[], byte[]> {

    private static final String VALUE_KEY_JSON_FMT = "{\"value\":%s,\"key\":%s}";
    protected final String quoted(String s) {
        return "\"" + s + "\"";
    }

    protected Map<String, String> getProducerProperties() {
        final Map<String, String> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return producerProps;
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void standardGrouping(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<String>> expectedBlobsAndContent = new HashMap<>();

        int cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
                K objectKey = getBlobName(partition, 0, compression);
                expectedBlobsAndContent.compute(objectKey, (k, v) -> v == null ? new ArrayList<>() : v).add(value);
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofMinutes(1));

        for (final K expectedBlobName : expectedBlobsAndContent.keySet()) {
            final List<String> blobContent = bucketAccessor.readAndDecodeLines(expectedBlobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields).trim())
                    .collect(Collectors.toList());
            assertThat(blobContent).containsExactlyInAnyOrderElementsOf(expectedBlobsAndContent.get(expectedBlobName));
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    @Disabled
    void groupByTimestampVariable(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(testTopic, 0, "key-0".getBytes(StandardCharsets.UTF_8),
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 1, "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 3, "key-4".getBytes(StandardCharsets.UTF_8),
                "value-4".getBytes(StandardCharsets.UTF_8)));

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        final Map<K, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getTimestampBlobName(0, 0, compression),
                new String[] { "key-0,value-0", "key-1,value-1", "key-2,value-2" });
        expectedBlobsAndContent.put(getTimestampBlobName(1, 0, compression), new String[] { "key-3,value-3" });
        expectedBlobsAndContent.put(getTimestampBlobName(3, 0, compression), new String[] { "key-4,value-4" });


        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofMinutes(1));


        for (final K expectedBlobName : expectedBlobsAndContent.keySet()) {
            final List<String> blobContent = bucketAccessor.readAndDecodeLines(expectedBlobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields).trim())
                    .collect(Collectors.toList());
            assertThat(blobContent).containsExactlyInAnyOrder(expectedBlobsAndContent.get(expectedBlobName));
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void oneFilePerRecordWithPlainValues(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression.name);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(testTopic, 0, "key-0".getBytes(StandardCharsets.UTF_8),
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 1, "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 3, "key-4".getBytes(StandardCharsets.UTF_8),
                "value-4".getBytes(StandardCharsets.UTF_8)));

        awaitFutures(sendFutures, Duration.ofSeconds(2));


        final Map<K, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getBlobName(0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getBlobName(0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getBlobName(0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getBlobName(1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getBlobName(3, 0, compression), "value-4");

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofMinutes(1));

        for (final Map.Entry<K, String> entry : expectedBlobsAndContent.entrySet()) {
            assertThat(bucketAccessor.readString(entry.getKey(), compression))
                    .isEqualTo(entry.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void groupByKey(final CompressionType compressionType) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("file.name.template", "{{key}}" + compressionType.extension());
        createConnector(connectorConfig);


        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(new TopicPartition(testTopic, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3", "key-7"));
        keysPerTopicPartition.put(new TopicPartition(testTopic, 1), Arrays.asList("key-4", "key-5", "key-6", "key-8"));
//        keysPerTopicPartition.put(new TopicPartition(testTopic, 0), Arrays.asList(null, "key-7"));
//        keysPerTopicPartition.put(new TopicPartition(testTopic, 1), Collections.singletonList("key-8"));

        final Map<K, Pair<byte[], String>> expectedBlobs = new TreeMap<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<String, String> lastValuePerKey = new HashMap<>();
        final int cntMax = 1000;
        int cnt = 0;
        outer : while (true) {
            for (final Map.Entry<TopicPartition, List<String>> entry : keysPerTopicPartition.entrySet()) {
                for (final String key : entry.getValue()) {
                    final String value = "value-" + cnt;
                    cnt += 1;
                    final byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
                    sendFutures.add(sendMessageAsync(entry.getKey().topic(), entry.getKey().partition(), keyBytes,
                            value.getBytes(StandardCharsets.UTF_8)));
                    expectedBlobs.put(getKeyBlobName(keyBytes, compressionType), Pair.of(keyBytes, value));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofMinutes(1));

        for (final K blobName : expectedBlobs.keySet()) {
            final String blobContent = bucketAccessor.readAndDecodeLines(blobName, compressionType, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());

            Pair<byte[], String> keyValue = expectedBlobs.get(blobName);
            assertThat(blobName).isEqualTo(getKeyBlobName(keyValue.getKey(), compressionType));
            assertThat(blobContent).isEqualTo(keyValue.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void jsonlOutput(CompressionType compressionType) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("format.output.type", contentType);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, String> expectedBlobsAndContent = new TreeMap<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = String.format("[{\"name\":\"user-%s\"}]", cnt);
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
                expectedBlobsAndContent.put(getBlobName(partition, i, compressionType), value);
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofMinutes(1));

        for (final Map.Entry<K, String> entry : expectedBlobsAndContent.entrySet()) {
            assertThat(bucketAccessor.readLines(entry.getKey(), compressionType)).containsExactly(entry.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void jsonOutput(CompressionType compressionType) throws ExecutionException, InterruptedException, IOException {
        //final var faultyProxy = enableFaultyProxy();
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String contentType = "json";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("format.output.type", contentType);
        createConnector(connectorConfig);

        final int numEpochs = 10;

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<String>> expectedBlobsAndContent = new TreeMap<>();

        int cnt = 0;
        for (int i = 0; i < numEpochs; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";

                sendFutures.add(sendMessageAsync(testTopic, partition, key.getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
                expectedBlobsAndContent.computeIfAbsent(getBlobName(partition, 0, compressionType), k -> new ArrayList<>()).add(String.format(VALUE_KEY_JSON_FMT, value, quoted(key)) + ",");
                cnt += 1;
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofHours(2));

        for (final Map.Entry<K, List<String>> entry : expectedBlobsAndContent.entrySet()) {
          List<String> lst = bucketAccessor.readLines(entry.getKey(), compressionType);
          // remove first and last entries because they are "[" and "]"
            lst.remove(0);
            lst.remove(lst.size() - 1);
            List<String> expected = entry.getValue();
            int pos = expected.size()-1;
            String lastEntry = expected.get(pos);
            lastEntry = lastEntry.substring(0, lastEntry.length() - 1);
            expected.set(pos, lastEntry);
            assertThat(lst).containsExactlyElementsOf(expected);
        }
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = getSinkStorage().createSinkProperties(bucketAccessor.bucketName);
        config.put("name", getSinkStorage().getConnectorClass().getSimpleName());
        config.put("connector.class", getSinkStorage().getConnectorClass().getName());
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        config.put("topics", testTopic);
        if (prefix != null) {
            config.put("file.name.prefix", prefix);
        }
        return config;
    }

//    private static WireMockServer enableFaultyProxy() {
//        final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
//        wireMockServer.start();
//        wireMockServer.addStubMapping(WireMock.request(RequestMethod.ANY.getName(), UrlPattern.ANY)
//                .willReturn(aResponse().proxiedFrom(gcsEndpoint))
//                .build());
//        final String urlPathPattern = "/upload/storage/v1/b/" + testBucketName + "/o";
//        wireMockServer.addStubMapping(
//                WireMock.request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
//                        .inScenario("temp-error")
//                        .willSetStateTo("Error")
//                        .willReturn(aResponse().withStatus(400))
//                        .build());
//        wireMockServer.addStubMapping(
//                WireMock.request(RequestMethod.POST.getName(), UrlPattern.fromOneOf(null, null, null, urlPathPattern))
//                        .inScenario("temp-error")
//                        .whenScenarioStateIs("Error")
//                        .willReturn(aResponse().proxiedFrom(gcsEndpoint))
//                        .build());
//        return wireMockServer;
//    }
}
