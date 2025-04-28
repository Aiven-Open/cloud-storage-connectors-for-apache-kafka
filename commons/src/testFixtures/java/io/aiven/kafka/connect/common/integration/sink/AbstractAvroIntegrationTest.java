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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class AbstractAvroIntegrationTest<N, K extends Comparable<K>>
        extends
            AbstractSinkIntegrationTest<N, K> {
    private static final KafkaProducer<String, GenericRecord> NULL_PRODUCER = null;
    private KafkaProducer<String, GenericRecord> producer;

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
        sinkStorage.removeStorage();
    }

    /**
     * Creates a configuration with the storage and general Avro configuration options.
     *
     * @param topics
     *            the topics to listend to.
     * @return ta configuration map.
     */
    @Override
    protected Map<String, String> createConfiguration(final String... topics) {
        final Map<String, String> config = super.createConfiguration(topics);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        return config;
    }

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        final List<Arguments> lst = new ArrayList<>();

        // String[] codecs = {"null", "deflate", "snappy", "bzip2", "xz", "zstandard"};
        // for (String codec : codecs) {
        // for (CompressionType compression : CompressionType.values()) {
        // lst.add(Arguments.of(codec, compression));
        // }
        // }
        lst.add(Arguments.of("null", CompressionType.NONE));
        lst.add(Arguments.of("bzip2", CompressionType.NONE));
        lst.add(Arguments.of("deflate", CompressionType.NONE));
        lst.add(Arguments.of("snappy", CompressionType.GZIP));
        lst.add(Arguments.of("zstandard", CompressionType.NONE));

        return lst.stream();
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutput(final String avroCodec, final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic(avroCodec, compression.name);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE)
                .withFormatType(FormatType.AVRO);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);
        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K nativeKey : expectedBlobs) {
            for (final GenericRecord r : AvroTestDataFixture.readAvroRecords(readBytes(nativeKey, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(mapF("message")).collect(Collectors.toList());
        String[] expected = expectedGenericRecords.stream()
                .map(mapF("message"))
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("id")).collect(Collectors.toList());
        expected = expectedGenericRecords.stream().map(mapF("id")).collect(Collectors.toList()).toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    @Test
    void jsonlAvroOutputTest() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        /*
         * connectorConfig.put("format.output.fields", "key,value");
         * connectorConfig.put("format.output.fields.value.encoding", "none"); connectorConfig.put("key.converter",
         * "io.confluent.connect.avro.AvroConverter"); connectorConfig.put("value.converter",
         * "io.confluent.connect.avro.AvroConverter"); connectorConfig.put("value.converter.schemas.enable", "false");
         * connectorConfig.put("file.compression.type", compression.name()); connectorConfig.put("format.output.type",
         * contentType);
         */

        kafkaManager.configureConnector(connectorName, connectorConfig);
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        // wait for them to show up.
        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();

        for (final K nativeKey : expectedBlobs) {
            for (final JsonNode node : JsonTestDataFixture.readJsonRecords(readBytes(nativeKey, compression))) {
                actualValues.add(node.findValue("message").asText());
            }
        }

        final String[] expected = expectedGenericRecords.stream().map(mapF("message")).toArray(String[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expected);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<GenericRecord> produceRecords(final int recordCountPerPartition, final int partitionCount,
            final String topicName) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount);
        int cnt = 0;
        for (final GenericRecord value : genericRecords) {
            final int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key, value)));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<GenericRecord> produceRecords(final int recordCountPerPartition, final int partitionCount,
            final String topicName, final Function<Integer, GenericRecord> func)
            throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount, func);
        int cnt = 0;
        for (final GenericRecord value : genericRecords) {
            final int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key, value)));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
    }

    private KafkaProducer<String, GenericRecord> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }

    // private Map<String, String> basicConnectorConfig(final String connectorName) {
    // final Map<String, String> config = new HashMap<>();
    // config.put("name", connectorName);
    // config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    // config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
    // config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    // config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
    // config.put("tasks.max", "1");
    // return config;
    // }

    // private Map<String, String> awsSpecificConfig(final Map<String, String> config, final String topicName) {
    // config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
    // config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
    // config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
    // config.put("aws.s3.endpoint", s3Endpoint);
    // config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
    // config.put("aws.s3.prefix", prefix);
    // config.put("topics", topicName);
    // config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
    // config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
    // config.put("tasks.max", "1");
    // return config;
    // }

    // private String getAvroBlobName(final String topicName, final int partition, final int startOffset,
    // final CompressionType compression) {
    // final String result = String.format("%s%s-%d-%020d.avro", prefix, topicName, partition, startOffset);
    // return result + compression.extension();
    // }

    // WARN: different from GCS
    // private String getBlobName(final String topicName, final int partition, final int startOffset,
    // final CompressionType compression) {
    // final String result = String.format("%s%s-%d-%020d", prefix, topicName, partition, startOffset);
    // return result + compression.extension();
    // }

    // private static final String CONNECTOR_NAME = "aiven-azure-sink-connector-avro";
    //
    // private final Schema avroInputDataSchema = new Schema.Parser().parse(
    // "{\"type\":\"record\",\"name\":\"input_data\"," + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");
    //
    //// @BeforeEach
    //// void setUp() throws ExecutionException, InterruptedException {
    //// testBlobAccessor.clear(azurePrefix);
    //// final Map<String, Object> producerProps = new HashMap<>();
    //// producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    //// producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    //// "io.confluent.kafka.serializers.KafkaAvroSerializer");
    //// producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    //// "io.confluent.kafka.serializers.KafkaAvroSerializer");
    //// producerProps.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
    //// startConnectRunner(producerProps);
    //// }
    //
    // private void produceRecords(final int recordCountPerPartition) throws ExecutionException, InterruptedException {
    // final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
    // int cnt = 0;
    // for (int i = 0; i < recordCountPerPartition; i++) {
    // for (int partition = 0; partition < 4; partition++) {
    // final String key = "key-" + cnt;
    // final GenericRecord value = new GenericData.Record(avroInputDataSchema);
    // value.put("name", "user-" + cnt);
    // cnt += 1;
    //
    // sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
    // }
    // }
    // getProducer().flush();
    // for (final Future<RecordMetadata> sendFuture : sendFutures) {
    // sendFuture.get();
    // }
    // }

    // @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    // @ParameterizedTest
    // @MethodSource("compressionAndCodecTestParameters")
    // void avroOutput(final String avroCodec, final CompressionType compression)
    // throws ExecutionException, InterruptedException, IOException {
    // final var topicName = getTopic() + "-" + avroCodec + "-" + compression;
    // final KafkaManager kafkaManager = getKafkaManager();
    // kafkaManager.createTopic(topicName);
    //
    // producer = newProducer();
    //
    // final Map<String, String> connectorConfig = avroSpecificConfig(
    // basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
    // connectorConfig.put("file.compression.type", compression.name());
    // connectorConfig.put("format.output.fields", "key,value");
    // connectorConfig.put("format.output.type", "avro");
    // connectorConfig.put("avro.codec", avroCodec);
    // kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
    //
    // final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
    // final int partitionCount = 4;
    // final int recordCountPerPartition = 10;
    //
    // assertThat(containerAccessor.getNativeStorage()).isEmpty();
    //
    // final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
    // topicName);
    //
    // // get list of expected blobs
    // final String[] expectedBlobs = { getAvroBlobName(topicName, 0, 0, compression),
    // getAvroBlobName(topicName, 1, 0, compression), getAvroBlobName(topicName, 2, 0, compression),
    // getAvroBlobName(topicName, 3, 0, compression) };
    //
    // waitForNativeStorage(timeout, containerAccessor::getNativeStorage, expectedBlobs);
    //
    // // extract all the actual records.
    // final List<GenericRecord> actualValues = new ArrayList<>();
    // final Function<GenericRecord, String> idMapper = mapF("id");
    // final Function<GenericRecord, String> messageMapper = mapF("message");
    //
    // for (final String blobName : expectedBlobs) {
    // for (final GenericRecord r : AvroTestDataFixture
    // .readAvroRecords(testBucketAccessor.readBytes(blobName, compression))) {
    // final GenericRecord value = (GenericRecord) r.get("value");
    // final String key = r.get("key").toString();
    // assertThat(key).isEqualTo("key-" + idMapper.apply(value));
    // assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
    // actualValues.add(value);
    // }
    // }
    //
    // List<String> values = actualValues.stream().map(mapF("message")).collect(Collectors.toList());
    // String[] expected = expectedGenericRecords.stream()
    // .map(mapF("message"))
    // .collect(Collectors.toList())
    // .toArray(new String[0]);
    //
    // assertThat(values).containsExactlyInAnyOrder(expected);
    //
    // values = actualValues.stream().map(mapF("id")).collect(Collectors.toList());
    // expected = expectedGenericRecords.stream().map(mapF("id")).collect(Collectors.toList()).toArray(new String[0]);
    //
    // assertThat(values).containsExactlyInAnyOrder(expected);
    // }

    // private String getAvroBlobName(final String topicName, final int partition, final int startOffset,
    // final CompressionType compression) {
    // final String result = String.format("%s%s-%d-%020d.avro", s3Prefix, topicName, partition, startOffset);
    // return result + compression.extension();
    // }

    @Test
    void defaultAvroOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = sinkStorage.getDefaultCompression();
        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(getNativeKeys()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final K nativeKey : expectedBlobs) {
            for (final GenericRecord r : AvroTestDataFixture.readAvroRecords(readBytes(nativeKey, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(messageMapper.apply(value));
            }
        }

        final List<String> expected = expectedGenericRecords.stream().map(mapF("message")).collect(Collectors.toList());

        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expected);

    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutputPlainValueWithoutEnvelope(final String avroCodec, final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {

        final String topicName = getTopic(avroCodec, compression.name);
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.VALUE)
                .envelopeEnabled(false);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        produceRecords(recordCountPerPartition, partitionCount, topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final List<GenericRecord> blobContents = new ArrayList<>();
        for (final K blobName : expectedBlobs) {
            blobContents.addAll(AvroTestDataFixture.readAvroRecords(readBytes(blobName, compression)));
        }

        final List<GenericRecord> enrichedValues = new ArrayList<>();
        enrichedValues.addAll(AvroTestDataFixture.generateAvroRecords(recordCountPerPartition * partitionCount,
                i -> AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.CONNECT_EXTRA_SCHEMA)));

        assertThat(enrichedValues).containsExactlyInAnyOrder(blobContents.toArray(GenericRecord[]::new));
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

        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();
        final CompressionType compression = sinkStorage.getDefaultCompression();

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.VALUE)
                .envelopeEnabled(false)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        kafkaManager.configureConnector(connectorName, connectorConfig);

        // Send only three records, assert three files created.
        final List<GenericRecord> expectedRecords = produceRecords(3, 1, topicName, i -> {
            if (i % 2 == 0) {
                final GenericRecord rec = AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.EVOLVED_SCHEMA);
                rec.put("age", i);
                return rec;
            }
            return AvroTestDataFixture.generateAvroRecord(i, AvroTestDataFixture.DEFAULT_SCHEMA);
        });

        final List<K> expectedBlobs = List.of(sinkStorage.getAvroBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 0, 1, compression),
                sinkStorage.getAvroBlobName(prefix, topicName, 0, 2, compression));

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        final var actualValues = new ArrayList<String>();
        for (final K blobName : expectedBlobs) {
            AvroTestDataFixture.readAvroRecords(readBytes(blobName, compression))
                    .forEach(genericRecord -> actualValues.add(genericRecord.toString()));
        }

        final List<String> expectedValues = expectedRecords.stream()
                .map(GenericRecord::toString)
                .collect(Collectors.toList());
        assertThat(actualValues).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    @Test
    void jsonlOutput() throws ExecutionException, InterruptedException, IOException {
        final String topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final CompressionType compression = CompressionType.NONE;

        final Map<String, String> connectorConfig = createConfiguration(topicName);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.JSONL)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);

        kafkaManager.configureConnector(connectorName, connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        final List<K> expectedBlobs = List.of(sinkStorage.getBlobName(prefix, topicName, 0, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 1, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 2, 0, compression),
                sinkStorage.getBlobName(prefix, topicName, 3, 0, compression));

        waitForStorage(timeout, this::getNativeKeys, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();

        for (final K nativeKey : expectedBlobs) {
            for (final JsonNode node : JsonTestDataFixture
                    .readJsonRecords(readBytes(nativeKey, CompressionType.NONE))) {
                actualValues.add(node.findValue("message").asText());
            }
        }

        final String[] expected = expectedGenericRecords.stream().map(mapF("message")).toArray(String[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expected);
    }
}
