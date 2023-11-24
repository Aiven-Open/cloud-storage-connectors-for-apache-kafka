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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AvroParquetIntegrationTest extends AbstractIntegrationTest<String, GenericRecord> {

    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector-parquet";

    @Container
    private final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(KAFKA);

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBucketAccessor.clear(gcsPrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        startConnectRunner(producerProps);
    }

    @Test
    void allOutputFields(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        final var compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        getConnectRunner().createConnector(connectorConfig);

        final Schema valueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema); // NOPMD instantiation in a loop
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = List.of(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetUtils.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, "none");
                final GenericRecord record = blobContents.get(blobName).get(i);
                final var expectedKey = "key-" + cnt;
                final var expectedValue = "{\"name\": \"" + name + "\", \"value\": \"" + value + "\"}";
                assertEquals(expectedKey, record.get("key").toString());
                assertEquals(expectedValue, record.get("value").toString());
                assertNotNull(record.get("offset"));
                assertNotNull(record.get("timestamp"));
                assertNull(record.get("headers"));
                cnt += 1;
            }
        }
    }

    @Test
    void valueComplexType(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        final String compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        getConnectRunner().createConnector(connectorConfig);

        final Schema valueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema); // NOPMD instantiation in a loop
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = List.of(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetUtils.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, "none");
                final GenericRecord record = blobContents.get(blobName).get(i);
                final var recordValue = (GenericRecord) record.get("value");
                assertEquals(name, recordValue.get("name").toString());
                assertEquals(value, recordValue.get("value").toString());
                cnt += 1;
            }
        }
    }

    @Test
    void schemaChanged(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        final String compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        getConnectRunner().createConnector(connectorConfig);

        final Schema valueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .endRecord();

        final Schema newValueSchema = SchemaBuilder.record("value")
                .fields()
                .name("name")
                .type()
                .stringType()
                .noDefault()
                .name("value")
                .type()
                .stringType()
                .noDefault()
                .name("blocked")
                .type()
                .booleanType()
                .booleanDefault(false)
                .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        final var expectedRecords = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value;
                if (i < 5) { // NOPMD literal
                    value = new GenericData.Record(valueSchema); // NOPMD instantiation in a loop
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                } else {
                    value = new GenericData.Record(newValueSchema); // NOPMD instantiation in a loop
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                    value.put("blocked", true);
                }
                expectedRecords.add(value.toString());
                cnt += 1;
                sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
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
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final var records = ParquetUtils.readRecords(tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName));
            blobContents.addAll(records.stream().map(r -> r.get("value").toString()).collect(Collectors.toList()));
        }
        assertIterableEquals(expectedRecords.stream().sorted().collect(Collectors.toList()),
                blobContents.stream().sorted().collect(Collectors.toList()));
    }

    private Map<String, String> basicConnectorConfig(final String compression) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", GcsSinkConnector.class.getName());
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
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
        config.put("file.name.prefix", gcsPrefix);
        config.put("topics", testTopic0 + "," + testTopic1);
        config.put("file.compression.type", compression);
        config.put("format.output.type", "parquet");
        return config;
    }

}
