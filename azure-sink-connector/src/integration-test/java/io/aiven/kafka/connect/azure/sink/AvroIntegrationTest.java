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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
@Testcontainers
final class AvroIntegrationTest extends AbstractIntegrationTest<String, GenericRecord> {
    private static final String CONNECTOR_NAME = "aiven-azure-sink-connector-avro";

    private final Schema avroInputDataSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"input_data\"," + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBlobAccessor.clear(azurePrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaManager().bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        startConnectRunner(producerProps);
    }

    private void produceRecords(final int recordCountPerPartition) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(avroInputDataSchema);
                value.put("name", "user-" + cnt);
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    @Test
    void avroOutput() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "avro");
        createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0), getAvroBlobName(1, 0),
                getAvroBlobName(2, 0), getAvroBlobName(3, 0));
        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        final Map<String, Schema> azureOutputAvroSchemas = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = testBlobAccessor.readBytes(blobName);
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    final List<GenericRecord> items = new ArrayList<>();
                    reader.forEach(items::add);
                    blobContents.put(blobName, items);
                    azureOutputAvroSchemas.put(blobName, reader.getSchema());
                }
            }
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String blobName = getAvroBlobName(partition, 0);
                final Schema azureOutputAvroSchema = azureOutputAvroSchemas.get(blobName);
                final GenericData.Record expectedRecord = new GenericData.Record(azureOutputAvroSchema);
                expectedRecord.put("key", new Utf8("key-" + cnt));
                final GenericData.Record valueRecord = new GenericData.Record(
                        azureOutputAvroSchema.getField("value").schema());
                valueRecord.put("name", new Utf8("user-" + cnt));
                expectedRecord.put("value", valueRecord);
                cnt += 1;

                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
                assertThat(actualRecord).isEqualTo(expectedRecord);
            }
        }
    }

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        return Stream.of(Arguments.of("bzip2", "none"), Arguments.of("deflate", "none"), Arguments.of("null", "none"),
                Arguments.of("snappy", "gzip"), // single test for codec and compression when both set.
                Arguments.of("zstandard", "none"));
    }

    private byte[] getBlobBytes(final byte[] blobBytes, final String compression) throws IOException {
        final CompressionType compressionType = CompressionType.forName(compression);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = compressionType.decompress(new ByteArrayInputStream(blobBytes))) {
            IOUtils.copy(inputStream, outputStream);
            return outputStream.toByteArray();
        }
    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutputPlainValueWithoutEnvelope(final String avroCodec, final String compression)
            throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_ENVELOPE_CONFIG, "false");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "avro");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put("avro.codec", avroCodec);
        createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0, compression),
                getAvroBlobName(1, 0, compression), getAvroBlobName(2, 0, compression),
                getAvroBlobName(3, 0, compression));
        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = getBlobBytes(testBlobAccessor.readBytes(blobName), compression);
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                final List<GenericRecord> items;
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    items = new ArrayList<>();
                    reader.forEach(items::add);
                }
                blobContents.put(blobName, items);
            }
        }

        // Connect will add two extra fields to schema and enrich it with
        // connect.version: 1
        // connect.name: input_data
        final Schema avroInputDataSchemaWithConnectExtra = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"input_data\","
                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}],"
                        + "\"connect.version\":1,\"connect.name\":\"input_data\"}");
        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String blobName = getAvroBlobName(partition, 0, compression);
                final GenericData.Record expectedRecord = new GenericData.Record(avroInputDataSchemaWithConnectExtra);
                expectedRecord.put("name", new Utf8("user-" + cnt));
                cnt += 1;

                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
                assertThat(actualRecord).isEqualTo(expectedRecord);
            }
        }
    }

    /**
     * When Avro schema changes a new Avro Container File must be produced to Azure. Avro Container File can have only
     * records written with same schema.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_ENVELOPE_CONFIG, "false");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "avro");
        createConnector(connectorConfig);

        final Schema evolvedAvroInputDataSchema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"input_data\","
                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\",\"default\":0}]}");

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final var expectedRecords = new ArrayList<String>();
        // Send only three records, assert three files created.
        for (int i = 0; i < 3; i++) {
            final var key = "key-" + i;
            final GenericRecord value;
            if (i % 2 == 0) { // NOPMD literal
                value = new GenericData.Record(avroInputDataSchema);
                value.put("name", new Utf8("user-" + i));
            } else {
                value = new GenericData.Record(evolvedAvroInputDataSchema);
                value.put("name", new Utf8("user-" + i));
                value.put("age", i);
            }
            expectedRecords.add(value.toString());
            sendFutures.add(sendMessageAsync(testTopic0, 0, key, value));
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0), getAvroBlobName(0, 1),
                getAvroBlobName(0, 2));

        awaitAllBlobsWritten(expectedBlobs.size());
        final List<String> blobNames = testBlobAccessor.getBlobNames(azurePrefix);
        assertThat(blobNames).isEqualTo(expectedBlobs);

        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = testBlobAccessor.readBytes(blobName);
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    reader.forEach(record -> blobContents.add(record.toString()));
                }
            }
        }
        assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void jsonlOutput() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String compression = "none";
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        connectorConfig.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        connectorConfig.put(AzureBlobSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");
        createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));
        awaitAllBlobsWritten(expectedBlobs.size());
        assertThat(testBlobAccessor.getBlobNames(azurePrefix)).containsExactlyElementsOf(expectedBlobs);

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBlobAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{" + "\"name\":\"user-" + cnt + "\"}";
                cnt += 1;

                final String blobName = getBlobName(partition, 0, "none");
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";
                assertThat(actualLine).isEqualTo(expectedLine);
            }
        }
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put(AzureBlobSinkConfig.NAME_CONFIG, CONNECTOR_NAME);
        config.put("connector.class", AzureBlobSinkConnector.class.getName());
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        if (useFakeAzure()) {
            config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, azureEndpoint);
        } else {
            config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, azureConnectionString);
        }
        config.put(AzureBlobSinkConfig.AZURE_STORAGE_CONTAINER_NAME_CONFIG, testContainerName);
        config.put(AzureBlobSinkConfig.FILE_NAME_PREFIX_CONFIG, azurePrefix);
        config.put("topics", testTopic0 + "," + testTopic1);
        return config;
    }

    protected String getAvroBlobName(final int partition, final int startOffset, final String compression) {
        return super.getBaseBlobName(partition, startOffset) + ".avro"
                + CompressionType.forName(compression).extension();
    }

    protected String getAvroBlobName(final int partition, final int startOffset) {
        return super.getBaseBlobName(partition, startOffset) + ".avro";
    }
}
