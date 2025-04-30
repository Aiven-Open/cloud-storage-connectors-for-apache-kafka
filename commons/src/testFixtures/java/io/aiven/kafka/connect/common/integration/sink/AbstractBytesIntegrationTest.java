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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.request;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.format.JsonTestDataFixture;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBytesIntegrationTest<N, K extends Comparable<K>>
        extends
            AbstractSinkIntegrationTest<N, K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBytesIntegrationTest.class);
    private static final KafkaProducer<byte[], byte[]> NULL_PRODUCER = null;
    protected KafkaProducer<byte[], byte[]> producer;

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
        sinkStorage.removeStorage();
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
    @EnumSource(CompressionType.class)
    void basicTest(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        prefix = createPrefix();
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());

        kafkaManager.configureConnector(connectorName, connectorConfig);
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
        final Collection<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // read the data
        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();

        for (final K blobName : expectedBlobs) {
            JsonTestDataFixture.readAndDecodeLines(readBytes(blobName, compression), 0, 1).forEach(fields -> {
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
    @EnumSource(CompressionType.class)
    void groupByTimestampVariable(final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", prefix + "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}");

        kafkaManager.configureConnector(connectorName, connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        produceRecords(recordOf(topicName, 0, "key-0", "value-0"), recordOf(topicName, 0, "key-1", "value-1"),
                recordOf(topicName, 0, "key-2", "value-2"), recordOf(topicName, 1, "key-3", "value-3"),
                recordOf(topicName, 3, "key-4", "value-4"));

        // get expected blobs
        final Map<K, List<String>> expectedBlobsAndContent = new TreeMap<>();
        expectedBlobsAndContent.put(sinkStorage.getTimestampBlobName(prefix, topicName, 0, 0),
                List.of("key-0,value-0", "key-1,value-1", "key-2,value-2"));
        expectedBlobsAndContent.put(sinkStorage.getTimestampBlobName(prefix, topicName, 1, 0),
                List.of("key-3,value-3"));
        expectedBlobsAndContent.put(sinkStorage.getTimestampBlobName(prefix, topicName, 3, 0),
                List.of("key-4,value-4"));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobsAndContent.keySet());

        for (final Map.Entry<K, List<String>> expected : expectedBlobsAndContent.entrySet()) {
            final List<String> blobContent = JsonTestDataFixture
                    .readAndDecodeLines(readBytes(expected.getKey(), compression), 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.toList());
            assertThat(blobContent).containsExactlyInAnyOrderElementsOf(expected.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void oneFilePerRecordWithPlainValues(final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + compression.name();
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");

        kafkaManager.configureConnector(connectorName, connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        produceRecords(recordOf(topicName, 0, "key-0", "value-0"), recordOf(topicName, 0, "key-1", "value-1"),
                recordOf(topicName, 0, "key-2", "value-2"), recordOf(topicName, 1, "key-3", "value-3"),
                recordOf(topicName, 3, "key-4", "value-4"));

        final Map<K, String> expectedBlobsAndContent = new TreeMap<>();
        expectedBlobsAndContent.put(sinkStorage.getNewBlobName(prefix, topicName, 0, 0, compression), "value-0");
        expectedBlobsAndContent.put(sinkStorage.getNewBlobName(prefix, topicName, 0, 1, compression), "value-1");
        expectedBlobsAndContent.put(sinkStorage.getNewBlobName(prefix, topicName, 0, 2, compression), "value-2");
        expectedBlobsAndContent.put(sinkStorage.getNewBlobName(prefix, topicName, 1, 0, compression), "value-3");
        expectedBlobsAndContent.put(sinkStorage.getNewBlobName(prefix, topicName, 3, 0, compression), "value-4");

        final Collection<K> expectedBlobs = expectedBlobsAndContent.keySet();

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        for (final Map.Entry<K, String> blobAndContentEntry : expectedBlobsAndContent.entrySet()) {
            assertThat(JsonTestDataFixture.readLines(readBytes(blobAndContentEntry.getKey(), compression)).get(0))
                    .isEqualTo(blobAndContentEntry.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void groupByKey(final CompressionType compression) throws ExecutionException, InterruptedException, IOException {
        final String topicName0 = getTopic() + "-" + compression.name();
        final String topicName1 = topicName0 + "_1";

        final Map<String, String> connectorConfig = createConfiguration(topicName0, topicName1);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", prefix + "{{key}}" + compression.extension());

        kafkaManager.configureConnector(connectorName, connectorConfig);
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

        final Collection<K> expectedBlobs = keysPerTopicPartition.values()
                .stream()
                .flatMap(keys -> keys.stream().map(key -> sinkStorage.getKeyBlobName(prefix, key, compression)))
                .collect(Collectors.toList());

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        for (final K blobName : expectedBlobs) {
            final String blobContent = JsonTestDataFixture.readAndDecodeLines(readBytes(blobName, compression), 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());
            final String keyInBlobName = blobName.toString().replace(prefix, "").replace(compression.extension(), "");
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
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(connectorName, connectorConfig);
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

        final Collection<K> expectedBlobs = List.of(sinkStorage.getNewBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final Map<K, List<String>> blobContents = new HashMap<>();
        for (final K blobName : expectedBlobs) {
            final List<String> items = Collections
                    .unmodifiableList(JsonTestDataFixture.readLines(readBytes(blobName, compression)));
            blobContents.put(blobName, items);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                final K blobName = sinkStorage.getNewBlobName(prefix, topicName, partition, 0, compression);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";

                assertThat(blobContents.get(blobName).get(i)).isEqualTo(expectedLine);
            }
        }
    }

    @Test
    void jsonOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        final CompressionType compression = CompressionType.NONE;
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        final String contentType = "json";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(connectorName, connectorConfig);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        final int epochs = 10;
        final int partitions = 4;

        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "[{" + "\"name\":\"user-" + currIdx + "\"}]";

        final List<KeyValueMessage> expectedRecords = produceRecords(partitions, epochs,
                new KeyValueGenerator(keyGen, valueGen), topicName);

        final Collection<K> expectedBlobs = List.of(sinkStorage.getNewBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getNewBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final Map<K, List<String>> blobContents = new HashMap<>();
        for (final K blobName : expectedBlobs) {
            final List<String> items = JsonTestDataFixture.readLines(readBytes(blobName, compression));
            assertThat(items).hasSize(epochs + 2);
            blobContents.put(blobName, Collections.unmodifiableList(items));
        }

        // Each blob must be a JSON array.
        for (final KeyValueMessage msg : expectedRecords) {
            final K blobName = sinkStorage.getNewBlobName(prefix, topicName, msg.partition, 0, compression);
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
        final WireMockServer faultyProxy = createFaultyProxy(sinkStorage, topicName);
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        if (sinkStorage.enableProxy(connectorConfig, faultyProxy)) {
            final String contentType = "json";
            connectorConfig.put("format.output.fields", "key,value");
            connectorConfig.put("format.output.fields.value.encoding", "none");
            connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
            connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            connectorConfig.put("value.converter.schemas.enable", "false");
            connectorConfig.put("file.compression.type", compression.name());
            connectorConfig.put("format.output.type", contentType);

            kafkaManager.configureConnector(connectorName, connectorConfig);
            kafkaManager.createTopic(topicName);
            producer = newProducer();

            final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 4);

            final int epochs = 10;
            final int partitions = 4;

            final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
            final IndexesToString valueGen = (partition, epoch, currIdx) -> "[{" + "\"name\":\"user-" + currIdx
                    + "\"}]";

            final List<KeyValueMessage> expectedRecords = produceRecords(partitions, epochs,
                    new KeyValueGenerator(keyGen, valueGen), topicName);

            final Collection<K> expectedBlobs = List.of(
                    sinkStorage.getNewBlobName(prefix, topicName, 0, 0, compression),
                    sinkStorage.getNewBlobName(prefix, topicName, 1, 0, compression),
                    sinkStorage.getNewBlobName(prefix, topicName, 2, 0, compression),
                    sinkStorage.getNewBlobName(prefix, topicName, 3, 0, compression));

            // wait for them to show up.
            waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

            final Map<K, List<String>> blobContents = new HashMap<>();
            for (final K blobName : expectedBlobs) {
                final List<String> items = JsonTestDataFixture.readLines(readBytes(blobName, compression));
                assertThat(items).hasSize(epochs + 2);
                blobContents.put(blobName, Collections.unmodifiableList(items));
            }

            // Each blob must be a JSON array.
            for (final KeyValueMessage msg : expectedRecords) {
                final K blobName = sinkStorage.getNewBlobName(prefix, topicName, msg.partition, 0, compression);
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
        } else {
            LOGGER.info("errorRecorvery test not supported");
        }

    }

    private static WireMockServer createFaultyProxy(final SinkStorage<?, ?> sinkStorage, final String topicName) {
        final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicHttpsPort());
        wireMockServer.start();
        wireMockServer.addStubMapping(request(RequestMethod.ANY.getName(), UrlPattern.ANY)
                .willReturn(aResponse().proxiedFrom(sinkStorage.getEndpointURL()))
                .build());
        final String urlPathPattern = sinkStorage.getURLPathPattern(topicName);
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
                        .willReturn(aResponse().proxiedFrom(sinkStorage.getEndpointURL()))
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

}
