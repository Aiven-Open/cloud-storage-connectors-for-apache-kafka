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

import io.aiven.kafka.connect.common.StringHelpers;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.format.AvroTestDataFixture;
import io.aiven.kafka.connect.common.format.JsonTestDataFixture;
import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSinkIntegrationTest<K extends Comparable<K>> extends AbstractSinkIntegrationBase<K, String, byte[]> {

    private static final String VALUE_KEY_JSON_FMT = "{\"value\":%s,\"key\":%s}";

    @Override
    protected final Map<String, Object> getProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        return props;
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void standardGrouping(final CompressionType compression) throws IOException {
        final FormatType formatType = FormatType.CSV;
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

                sendFutures.add(sendMessageAsync(testTopic, partition, key,
                        value.getBytes(StandardCharsets.UTF_8)));
                K objectKey = getNativeKey(partition, 0, compression, formatType);
                expectedBlobsAndContent.compute(objectKey, (k, v) -> v == null ? new ArrayList<>() : v).add(String.format("%s,%s", key, value));
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofSeconds(10));

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
    void groupByTimestampVariable(final CompressionType compression) throws IOException {
        final FormatType formatType = FormatType.CSV;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(testTopic, 0, "key-0",
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-1",
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-2",
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 1, "key-3",
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 3, "key-4",
                "value-4".getBytes(StandardCharsets.UTF_8)));

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        final Map<K, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getNativeKeyForTimestamp(0, 0, compression, formatType),
                new String[] { "key-0,value-0", "key-1,value-1", "key-2,value-2" });
        expectedBlobsAndContent.put(getNativeKeyForTimestamp(1, 0, compression, formatType), new String[] { "key-3,value-3" });
        expectedBlobsAndContent.put(getNativeKeyForTimestamp(3, 0, compression, formatType), new String[] { "key-4,value-4" });


        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofSeconds(10));


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
    void oneRecordPerFileWithPlainValues(final CompressionType compression) throws IOException {
        final FormatType formatType = FormatType.CSV;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression.name);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(testTopic, 0, "key-0",
                "value-0".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-1",
                "value-1".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 0, "key-2",
                "value-2".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 1, "key-3",
                "value-3".getBytes(StandardCharsets.UTF_8)));
        sendFutures.add(sendMessageAsync(testTopic, 3, "key-4",
                "value-4".getBytes(StandardCharsets.UTF_8)));

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        final Map<K, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getNativeKey(0, 0, compression, formatType), "value-0");
        expectedBlobsAndContent.put(getNativeKey(0, 1, compression, formatType), "value-1");
        expectedBlobsAndContent.put(getNativeKey(0, 2, compression, formatType), "value-2");
        expectedBlobsAndContent.put(getNativeKey(1, 0, compression, formatType), "value-3");
        expectedBlobsAndContent.put(getNativeKey(3, 0, compression, formatType), "value-4");

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofSeconds(45));

        for (final Map.Entry<K, String> entry : expectedBlobsAndContent.entrySet()) {
            assertThat(bucketAccessor.readString(entry.getKey(), compression))
                    .isEqualTo(entry.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void groupByKey(final CompressionType compressionType) throws IOException {
        final FormatType formatType = FormatType.CSV;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("file.name.template", "{{key}}" + compressionType.extension());
        createConnector(connectorConfig);


        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(new TopicPartition(testTopic, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3", "key-7"));
        keysPerTopicPartition.put(new TopicPartition(testTopic, 1), Arrays.asList("key-4", "key-5", "key-6", "key-8"));

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
                    sendFutures.add(sendMessageAsync(entry.getKey().topic(), entry.getKey().partition(), key,
                            value.getBytes(StandardCharsets.UTF_8)));
                    expectedBlobs.put(getNativeKeyForKey(keyBytes, compressionType, formatType), Pair.of(keyBytes, String.format("%s,%s",key, value)));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));

        for (final K blobName : expectedBlobs.keySet()) {
            final String blobContent = bucketAccessor.readAndDecodeLines(blobName, compressionType, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());

            Pair<byte[], String> keyValue = expectedBlobs.get(blobName);
            assertThat(blobName).isEqualTo(getNativeKeyForKey(keyValue.getKey(), compressionType, formatType));
            assertThat(blobContent).isEqualTo(keyValue.getValue());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void jsonlOutput(CompressionType compressionType) throws IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final FormatType contentType = FormatType.JSONL;
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("format.output.type", contentType.name);
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<String>> expectedBlobsAndContent = new TreeMap<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = String.format("[{\"name\":\"user-%s\"}]", cnt);
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic, partition, key,
                        value.getBytes(StandardCharsets.UTF_8)));
                expectedBlobsAndContent.computeIfAbsent(getNativeKey(partition, 0, compressionType, contentType), k -> new ArrayList<>())
                        .add(String.format(VALUE_KEY_JSON_FMT, value, StringHelpers.quoted(key)));
            }
        }

        awaitFutures(sendFutures, Duration.ofSeconds(2));

        awaitAllBlobsWritten(expectedBlobsAndContent.keySet(), Duration.ofSeconds(45));

        for (final Map.Entry<K, List<String>> entry : expectedBlobsAndContent.entrySet()) {
            assertThat(bucketAccessor.readLines(entry.getKey(), compressionType)).containsExactlyElementsOf(entry.getValue());
        }
    }

    private List<String> removeCommaFromLastEntry(List<String> strings) {
        int pos = strings.size()-1;
        String lastEntry = strings.get(pos);
        lastEntry = lastEntry.substring(0, lastEntry.length() - 1);
        strings.set(pos, lastEntry);
        return strings;
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void jsonOutput(CompressionType compressionType) throws IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final FormatType contentType = FormatType.JSON;
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("format.output.type", contentType.name);
        createConnector(connectorConfig);

        final int numEpochs = 10;

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<String>> expectedBlobsAndContent = new TreeMap<>();

        int cnt = 0;
        for (int i = 0; i < numEpochs; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";

                sendFutures.add(sendMessageAsync(testTopic, partition, key,
                        value.getBytes(StandardCharsets.UTF_8)));
                expectedBlobsAndContent.computeIfAbsent(getNativeKey(partition, 0, compressionType, contentType), k -> new ArrayList<>())
                        .add(String.format(VALUE_KEY_JSON_FMT, value, StringHelpers.quoted(key)) + ",");
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
            assertThat(lst).containsExactlyElementsOf(removeCommaFromLastEntry(entry.getValue()));
        }
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

    private class Record {
        String key;
        byte[] value;
        int partition;

        Record(String key, byte[] value, int partition) {
            this.key = key;
            this.value = value;
            this.partition = partition;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }
    }

    /**
     * Sends a batch of messages
     * @param recordCount the number of records to send.
     * @param partitionCount the number of partitions to spread them across.
     * @param valueGenerator the value of the record.
     * @param recGenerator the function to generate the expected contents ({@code <R>}) based on the recorc number
     * @param nativeKeyGenerator a function to generate the native Key based on the record number
     * @param timeout the time to wait for all the messages to be sent to Kafka
     * @return A map of native key to a list of {@code <R>}
     * @param <R> The return record type.
     */
    private <R> Map<K, List<R>> sendMessages(int recordCount, int partitionCount, IntFunction<byte[]> valueGenerator, IntFunction<R> recGenerator,
                                             IntFunction<K> nativeKeyGenerator, Duration timeout) {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<R>> expectedBlobsAndContent = new TreeMap<>();
        for (int recordNumber = 0; recordNumber < recordCount; recordNumber++) {
            int partition = recordNumber % partitionCount;
            final String key = "key-"+recordNumber;
            final byte[] value = valueGenerator.apply(recordNumber);
            sendFutures.add(sendMessageAsync(testTopic, partition, key, value));
            expectedBlobsAndContent.computeIfAbsent(nativeKeyGenerator.apply(recordNumber), k -> new ArrayList<>())
                    .add(recGenerator.apply(recordNumber));
        }
        awaitFutures(sendFutures, timeout);
        return expectedBlobsAndContent;
    }

    /**
     * Create one key for each partition.
     * @param partitionCount the number of partitions.
     * @param compressionType the compression type.
     * @param contentType the content format type.
     * @return A native key
     */
    private IntFunction<K> partitionZeroOffsetKey(int partitionCount, CompressionType compressionType, FormatType contentType) {
        return i -> getNativeKey(i % partitionCount, 0, compressionType, contentType);
    }

    private static IntFunction<byte[]> byteValueGenerator =  i -> ("value-"+i).getBytes(StandardCharsets.UTF_8);


    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void parquetOutput(CompressionType compressionType, @TempDir Path tempDir) throws IOException {
        final FormatType contentType = FormatType.PARQUET;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", StringConverter.class.getName());
        //connectorConfig.put("value.converter", Converter.class.getName());
        connectorConfig.put("format.output.type", contentType.name);
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        int partitionCount = 4;
        final Map<K, List<Integer>> expectedContents = sendMessages(40, partitionCount, byteValueGenerator, i -> i,
                partitionZeroOffsetKey(partitionCount, compressionType, contentType), Duration.ofSeconds(5));

        awaitAllBlobsWritten(expectedContents.keySet(), Duration.ofSeconds(45));

        for (K key : expectedContents.keySet()) {
            String blobName = key.toString();
            List<GenericRecord> records =  ParquetTestDataFixture.readRecords(tempDir.resolve(Paths.get(blobName)),
                    bucketAccessor.readBytes(key));
            List<Integer> expectedRecords = expectedContents.get(key);
            assertThat(records).hasSize(expectedRecords.size());
            for (int i = 0; i < expectedRecords.size(); i++) {
                GenericRecord record = records.get(i);
                int counter = expectedRecords.get(i);
                assertThat(record.get("key")).hasToString("key-" + counter);
                assertThat(((ByteBuffer)record.get("value")).array()).isEqualTo(byteValueGenerator.apply(counter));
                assertThat(record.get("offset")).isEqualTo((long)i);
               assertThat(record.get("timestamp")).isNotNull();
               assertThat(record.get("headers")).isNull();
            }
        }
    }

    private static IntFunction<byte[]> jsonValueGenerator =  i -> String.format("{\"name\": \"name-%1$s\", \"value\": \"value-%1$s\"}", i).getBytes(StandardCharsets.UTF_8);

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void parquetJsonValueAsString(CompressionType compressionType, @TempDir Path tempDir) throws IOException {
        final FormatType contentType = FormatType.PARQUET;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.type", contentType.name);
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        int partitionCount = 4;
        final Map<K, List<Integer>> expectedContents = sendMessages(40, partitionCount, jsonValueGenerator, i -> i,
                partitionZeroOffsetKey(partitionCount, compressionType, contentType),  Duration.ofSeconds(5));

        awaitAllBlobsWritten(expectedContents.keySet(), Duration.ofSeconds(45));

        for (K key : expectedContents.keySet()) {
            String blobName = key.toString();
            List<GenericRecord> records = ParquetTestDataFixture.readRecords(tempDir.resolve(Paths.get(blobName)),
                    bucketAccessor.readBytes(key));
            List<Integer> expectedRecords = expectedContents.get(key);
            assertThat(records).hasSize(expectedRecords.size());
            for (int i = 0; i < expectedRecords.size(); i++) {
                GenericRecord record = records.get(i);
                int counter = expectedRecords.get(i);
                assertThat(record.get("key")).hasToString("key-" + counter);
                assertThat(((ByteBuffer) record.get("value")).array()).isEqualTo(jsonValueGenerator.apply(counter));
                assertThat(record.get("offset")).isEqualTo((long) i);
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
            }
        }
    }

    private static IntFunction<byte[]> JsonFmt = i -> String.format("{\"schema\": %s, \"payload\": %s}", JsonTestDataFixture.SCHEMA_JSON, JsonTestDataFixture.generateJsonRec(i, "user-" + i)).getBytes(StandardCharsets.UTF_8);

//    @ParameterizedTest
//    @EnumSource(CompressionType.class)
//    @CsvSource({ "true, {\"value\": {\"name\": \"%s\"}} ", "false, {\"name\": \"%s\"}" })
    @Test
    void parquetJsonValue(@TempDir Path tempDir/*final String envelopeEnabled, final String expectedOutput*/)
            throws IOException {
        final CompressionType compressionType = CompressionType.NONE;
        final FormatType contentType = FormatType.PARQUET;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.envelope", "true");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("format.output.type", contentType.name);
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        int partitionCount = 4;
        final Map<K, List<Integer>> expectedContents = sendMessages(40, partitionCount, JsonFmt, i -> i,
                partitionZeroOffsetKey(partitionCount, compressionType, contentType), Duration.ofSeconds(5));

        awaitAllBlobsWritten(expectedContents.keySet(), Duration.ofSeconds(45));

        for (K key : expectedContents.keySet()) {
            String blobName = key.toString();
            List<GenericRecord> records = ParquetTestDataFixture.readRecords(tempDir.resolve(Paths.get(blobName)),
                    bucketAccessor.readBytes(key));
            List<Integer> expectedRecords = expectedContents.get(key);
            assertThat(records).hasSize(expectedRecords.size());
            for (int i = 0; i < expectedRecords.size(); i++) {
                GenericRecord record = records.get(i);
                int counter = expectedRecords.get(i);
                GenericRecord value = (GenericRecord) record.get("value");
                assertThat(value.get("id")).isEqualTo(counter);
                assertThat(value.get("message").toString()).isEqualTo("user-" + counter);
                assertThat(value.hasField("value")).isFalse();
            }
        }
    }

//        //final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
//        //
//        // final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";
//
//        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
//        int cnt = 0;
//        for (int i = 0; i < 10; i++) {
//            for (int partition = 0; partition < 4; partition++) {
//                final String key = "key-" + cnt;
//                final String value = String.format(jsonMessagePattern, JsonTestDataFixture.SCHEMA_JSON,
//                        "{" + "\"name\":\"user-" + cnt + "\"}");
//                cnt += 1;
//
//                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
//                        value.getBytes(StandardCharsets.UTF_8)));
//            }
//        }
//        getProducer().flush();
//        for (final Future<RecordMetadata> sendFuture : sendFutures) {
//            sendFuture.get();
//        }
//
//        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
//                getBlobName(2, 0, compression), getBlobName(3, 0, compression));
//
//        awaitAllBlobsWritten(expectedBlobs.size());
//        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);
//
//        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
//        for (final String blobName : expectedBlobs) {
//            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
//                    testBucketAccessor.readBytes(blobName));
//            blobContents.put(blobName, records);
//        }
//        cnt = 0;
//        for (int i = 0; i < 10; i++) {
//            for (int partition = 0; partition < 4; partition++) {
//                final var name = "user-" + cnt;
//                final String blobName = getBlobName(partition, 0, compression);
//                final var record = blobContents.get(blobName).get(i);
//                final String expectedLine = String.format(expectedOutput, name);
//                assertThat(record).hasToString(expectedLine);
//                cnt += 1;
//            }
//        }
//    }

    private static IntFunction<byte[]> EvolvedJsonFmt = i -> String.format("{\"schema\": %s, \"payload\": %s}", JsonTestDataFixture.EVOLVED_SCHEMA_JSON, JsonTestDataFixture.generateEvolvedJsonRec(i, "user-" + i)).getBytes(StandardCharsets.UTF_8);

    @Test
    void parquetSchemaChanged(@TempDir Path tempDir) throws IOException {
        final CompressionType compressionType = CompressionType.NONE;
        final FormatType contentType = FormatType.PARQUET;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("format.output.type", contentType.name);
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        IntFunction<byte[]> alternatingSchema = i -> i % 2 == 1 ? EvolvedJsonFmt.apply(i) : JsonFmt.apply(i);
        int partitionCount = 1;

        IntFunction<K> nativeKeyGenerator = i -> getNativeKey(i % partitionCount, i, compressionType, contentType);
        final Map<K, List<Integer>> expectedContents = sendMessages(3, partitionCount, alternatingSchema, i -> i,
                nativeKeyGenerator, Duration.ofSeconds(5));

        // since each record changes the schema each record should be in its own partition.
        final List<K> expectedKeys = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            expectedKeys.add(nativeKeyGenerator.apply(i));
        }


        awaitAllBlobsWritten(expectedKeys, Duration.ofSeconds(45));

        for (int i = 0; i < expectedKeys.size(); i++) {
            K key = expectedKeys.get(i);
            assertThat(expectedContents.get(key).size()).isEqualTo(1);
            int counter = expectedContents.get(key).get(0);
            List<GenericRecord> records = ParquetTestDataFixture.readRecords(tempDir.resolve(Paths.get(key.toString())),
                    bucketAccessor.readBytes(key));
            assertThat(records).size().isEqualTo(1);
            boolean newSchema = (i % 2) == 1;
            GenericRecord record = records.get(0);

            GenericRecord value = (GenericRecord) record.get("value");
            assertThat(value.get("id")).isEqualTo(counter);
            assertThat(value.get("message").toString()).isEqualTo("user-" + counter);
            assertThat(value.hasField("value")).isFalse();
            if (newSchema) {
                assertThat(value.get("age")).isEqualTo(counter * 10);
            } else {
                assertThat(value.hasField("age")).isFalse();
            }
        }

//        for (int i = 0; i < expectedKeys.size(); i++) {
//            final List<GenericRecord> items = serializer.extractGenericRecord(bucketAccessor.readBytes(expectedKeys.get(i)));
//            assertThat(items).size().isEqualTo(1);
//            boolean newSchema = (i % 2) == 1;
//            GenericRecord record = items.get(0);
//            assertThat(record.get("id")).isEqualTo(i);
//            assertThat(record.hasField("message")).isTrue();
//            if (newSchema) {
//                assertThat(record.get("age")).isEqualTo(i*10);
//            } else {
//                assertThat(record.hasField("age")).isFalse();
//            }
//            Map<String, Object> props = record.getSchema().getObjectProps();
//            assertThat(record.getSchema().getObjectProp("connect.version")).isEqualTo( newSchema ? 2 : 1);
//            assertThat(record.getSchema().getObjectProp("connect.name")).isEqualTo("TestRecord");
//        }

//        // write one record with old schema, one with new, and one with old again.  All written into partition 0
//        List<AbstractSinkAvroIntegrationTest.Record> avroRecords = serializer.sendRecords(3, 1, Duration.ofSeconds(3), (i) -> {
//                    // new schema every 3 records
//                    boolean newSchema = (i % 2) == 1;
//                    GenericRecord value = new GenericData.Record(newSchema ? AvroTestDataFixture.EVOLVED_SCHEMA : AvroTestDataFixture.DEFAULT_SCHEMA);
//                    value.put("message", new Utf8("user-" + i));
//                    value.put("id", i);
//                    if (newSchema) {
//                        value.put("age", i * 10);
//                    }
//                    return value;
//                }
//        );

//        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
//        final var jsonMessageNewSchema = "{\"type\":\"struct\",\"fields\":"
//                + "[{\"type\":\"string\",\"field\":\"name\"}, "
//                + "{\"type\":\"string\",\"field\":\"value\", \"default\": \"foo\"}]}";
//        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

//        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
//        final var expectedRecords = new ArrayList<String>();
//        int cnt = 0;
//        for (int i = 0; i < 10; i++) {
//            for (int partition = 0; partition < 4; partition++) {
//                final var key = "key-" + cnt;
//                final String value;
//                final String payload;
//                if (i < 5) { // NOPMD literal
//                    payload = "{" + "\"name\": \"user-" + cnt + "\"}";
//                    value = String.format(jsonMessagePattern, jsonMessageSchema, payload);
//                } else {
//                    payload = "{" + "\"name\": \"user-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
//                    value = String.format(jsonMessagePattern, jsonMessageNewSchema, payload);
//                }
//                expectedRecords.add(String.format("{\"value\": %s}", payload));
//                sendFutures.add(sendMessageAsync(testTopic0, partition, key.getBytes(StandardCharsets.UTF_8),
//                        value.getBytes(StandardCharsets.UTF_8)));
//                cnt += 1;
//            }
//        }
//        getProducer().flush();
//        for (final Future<RecordMetadata> sendFuture : sendFutures) {
//            sendFuture.get();
//        }
//
//        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(0, 5, compression),
//                getBlobName(1, 0, compression), getBlobName(1, 5, compression), getBlobName(2, 0, compression),
//                getBlobName(2, 5, compression), getBlobName(3, 0, compression), getBlobName(3, 5, compression));
//
//        awaitAllBlobsWritten(expectedBlobs.size());
//        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);
//
//        final var blobContents = new ArrayList<String>();
//        for (final String blobName : expectedBlobs) {
//            final var records = ParquetTestDataFixture.readRecords(tmpDir.resolve(Paths.get(blobName)),
//                    testBucketAccessor.readBytes(blobName));
//            blobContents.addAll(records.stream().map(GenericRecord::toString).collect(Collectors.toList()));
//        }
//        assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

}
