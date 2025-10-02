package io.aiven.kafka.connect.common.integration.sink;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.format.AvroTestDataFixture;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSinkAvroIntegrationTest<K extends Comparable<K>, N> extends AbstractSinkIntegrationBase<K> {

    public enum AvroCodec { NULL, DEFLATE, BZIP2, SNAPPY, XZ, ZSTANDARD};


    /**
     * A record of the key, value and partition as written to kafka.
     * Equality is only checked against key and value.
     */
    public static class Record implements Comparable<Record> {
        private final byte[] key;
        private final byte[] value;
        private final int partition;

        /**
         * Constructor
         * @param key the key.
         * @param value the value
         * @param partition the partition
         */
        public Record(byte[] key, byte[] value, int partition) {
            this.key = key;
            this.value = value;
            this.partition = partition;
        }

        /**
         * Gest the key value
         * @return the key value.
         */
        public byte[] getKey() {
            return key;
        }

        /**
         * Gets the value.
         * @return the value.
         */
        public byte[] getValue() {
            return value;
        }

        /**
         * Gets the partition.
         * @return the partition.
         */
        public int getPartition() {
            return partition;
        }

        @Override
        public int compareTo(Record o) {
            int result = Arrays.compare(key, o.key);
            return result == 0 ? Arrays.compare(value, o.value) : result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Record) {
                return this.compareTo((Record) o) == 0;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    @Test
    void avroOutput() throws IOException {
        final AvroSerDe serializer = new AvroSerDe();
        CompressionType compressionType = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compressionType.name);
        connectorConfig.put("format.output.type", FormatType.AVRO.name);
        createConnector(connectorConfig);

        List<Record> avroRecords = serializer.sendRecords(40, Duration.ofSeconds(45));

        final Map<K, List<Record>> expectedBlobs = new HashMap<>();
        avroRecords.forEach(record -> {
            expectedBlobs.compute(getNativeKey(record.getPartition(), 0, CompressionType.NONE, FormatType.AVRO), (k, v) -> v == null ? new ArrayList<>() : v)
                    .add(record);
        });

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));

        for (final K nativeKey : expectedBlobs.keySet()) {
            final List<Record> items = serializer.extractRecords(bucketAccessor.readBytes(nativeKey));
            assertThat(items).containsExactlyInAnyOrderElementsOf(expectedBlobs.get(nativeKey));
        }
    }

//
//    private static Stream<Arguments> compressionAndCodecTestParameters() {
//        return Stream.of(Arguments.of("bzip2", CompressionType.NONE), Arguments.of("deflate", CompressionType.NONE),
//                Arguments.of("null", CompressionType.NONE), Arguments.of("snappy", CompressionType.GZIP), // single test
//                // for codec
//                // and
//                // compression
//                // when both
//                // set.
//                Arguments.of("zstandard", CompressionType.NONE));
//    }
//
//    private byte[] getBlobBytes(final byte[] blobBytes, final CompressionType compression) throws IOException {
//        try (InputStream decompressedStream = compression.decompress(new ByteArrayInputStream(blobBytes));
//             ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
//            IOUtils.copy(decompressedStream, decompressedBytes);
//            return decompressedBytes.toByteArray();
//        }
//    }
//
//    @ParameterizedTest
//    @MethodSource("compressionAndCodecTestParameters")
//    void avroOutputPlainValueWithoutEnvelope(final String avroCodec, final CompressionType compression)
//            throws ExecutionException, InterruptedException, IOException {
//        final Map<String, String> connectorConfig = basicConnectorConfig();
//        connectorConfig.put("format.output.envelope", "false");
//        connectorConfig.put("format.output.fields", "value");
//        connectorConfig.put("format.output.type", "avro");
//        connectorConfig.put("file.compression.type", compression.name);
//        connectorConfig.put("avro.codec", avroCodec);
//        createConnector(connectorConfig);
//
//        final int recordCountPerPartition = 10;
//        produceRecords(recordCountPerPartition);
//
//        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0, compression),
//                getAvroBlobName(1, 0, compression), getAvroBlobName(2, 0, compression),
//                getAvroBlobName(3, 0, compression));
//        awaitAllBlobsWritten(expectedBlobs.size());
//        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);
//
//        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
//        for (final String blobName : expectedBlobs) {
//            final byte[] blobBytes = getBlobBytes(testBucketAccessor.readBytes(blobName), compression);
//            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
//                final List<GenericRecord> items;
//                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
//                    items = new ArrayList<>();
//                    reader.forEach(items::add);
//                }
//                blobContents.put(blobName, items);
//            }
//        }
//
//        // Connect will add two extra fields to schema and enrich it with
//        // connect.version: 1
//        // connect.name: input_data
//        final Schema avroInputDataSchemaWithConnectExtra = new Schema.Parser()
//                .parse("{\"type\":\"record\",\"name\":\"input_data\","
//                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}],"
//                        + "\"connect.version\":1,\"connect.name\":\"input_data\"}");
//        int cnt = 0;
//        for (int i = 0; i < recordCountPerPartition; i++) {
//            for (int partition = 0; partition < 4; partition++) {
//                final String blobName = getAvroBlobName(partition, 0, compression);
//                final GenericData.Record expectedRecord = new GenericData.Record(avroInputDataSchemaWithConnectExtra);
//                expectedRecord.put("name", new Utf8("user-" + cnt));
//                cnt += 1;
//
//                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
//                assertThat(actualRecord).isEqualTo(expectedRecord);
//            }
//        }
//    }
//
//    /**
//     * When Avro schema changes a new Avro Container File must be produced to GCS. Avro Container File can have only
//     * records written with same schema.
//     */
    @Test
    void schemaChanged() throws IOException {
        final AvroSerDe serializer = new AvroSerDe();
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.envelope", "false");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("format.output.type", FormatType.AVRO.name);
        createConnector(connectorConfig);

        List<Record> avroRecords = serializer.sendRecords(3, 1, Duration.ofSeconds(3), (i) -> {
                    // new schema every 3 records
                    boolean newSchema = (i % 2) == 1;
                    GenericRecord value = new GenericData.Record(newSchema ? AvroTestDataFixture.EVOLVED_SCHEMA : AvroTestDataFixture.DEFAULT_SCHEMA);
                    value.put("message", new Utf8("user-" + i));
                    value.put("id", i);
                    if (newSchema) {
                        value.put("age", i * 10);
                    }
                    return value;
                }
        );


        final Map<K, Record> expectedBlobs = new HashMap<>();
        for (int i = 0; i < 3; i++)
        {
            Record record = avroRecords.get(i);
            expectedBlobs.put(getNativeKey(record.getPartition(), 0, CompressionType.NONE, FormatType.AVRO), record);
        }

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));

        for (final K nativeKey : expectedBlobs.keySet()) {
            final List<Record> items = serializer.extractRecords(bucketAccessor.readBytes(nativeKey));
            assertThat(items).containsExactlyInAnyOrderElementsOf(Collections.singletonList(expectedBlobs.get(nativeKey)));
        }
    }

//
//        final Schema evolvedAvroInputDataSchema = new Schema.Parser()
//                .parse("{\"type\":\"record\",\"name\":\"input_data\","
//                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\",\"default\":0}]}");
//
//        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
//        final var expectedRecords = new ArrayList<String>();
//        // Send only three records, assert three files created.
//        for (int i = 0; i < 3; i++) {
//            final var key = "key-" + i;
//            final GenericRecord value;
//            if (i % 2 == 0) { // NOPMD literal
//                value = new GenericData.Record(avroInputDataSchema);
//                value.put("name", new Utf8("user-" + i));
//            } else {
//                value = new GenericData.Record(evolvedAvroInputDataSchema);
//                value.put("name", new Utf8("user-" + i));
//                value.put("age", i);
//            }
//            expectedRecords.add(value.toString());
//            sendFutures.add(sendMessageAsync(testTopic0, 0, key, value));
//        }
//        getProducer().flush();
//        for (final Future<RecordMetadata> sendFuture : sendFutures) {
//            sendFuture.get();
//        }
//
//        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0), getAvroBlobName(0, 1),
//                getAvroBlobName(0, 2));
//
//        awaitAllBlobsWritten(expectedBlobs.size());
//        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);
//
//        final var blobContents = new ArrayList<String>();
//        for (final String blobName : expectedBlobs) {
//            final byte[] blobBytes = testBucketAccessor.readBytes(blobName);
//            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
//                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
//                    reader.forEach(record -> blobContents.add(record.toString()));
//                }
//            }
//        }
//        assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
//    }
//
//    @Test
//    void jsonlOutput() throws ExecutionException, InterruptedException {
//        final Map<String, String> connectorConfig = basicConnectorConfig();
//        final String compression = "none";
//        connectorConfig.put("format.output.fields", "key,value");
//        connectorConfig.put("format.output.fields.value.encoding", "none");
//        connectorConfig.put("file.compression.type", compression);
//        connectorConfig.put("format.output.type", "jsonl");
//        createConnector(connectorConfig);
//
//        final int recordCountPerPartition = 10;
//        produceRecords(recordCountPerPartition);
//
//        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
//                getBlobName(2, 0, compression), getBlobName(3, 0, compression));
//
//        awaitAllBlobsWritten(expectedBlobs.size());
//        assertThat(testBucketAccessor.getBlobNames(gcsPrefix)).containsExactlyElementsOf(expectedBlobs);
//
//        final Map<String, List<String>> blobContents = new HashMap<>();
//        for (final String blobName : expectedBlobs) {
//            final List<String> items = new ArrayList<>(testBucketAccessor.readLines(blobName, compression));
//            blobContents.put(blobName, items);
//        }
//
//        int cnt = 0;
//        for (int i = 0; i < recordCountPerPartition; i++) {
//            for (int partition = 0; partition < 4; partition++) {
//                final String key = "key-" + cnt;
//                final String value = "{" + "\"name\":\"user-" + cnt + "\"}";
//                cnt += 1;
//
//                final String blobName = getBlobName(partition, 0, "none");
//                final String actualLine = blobContents.get(blobName).get(i);
//                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";
//                assertThat(actualLine).isEqualTo(expectedLine);
//            }
//        }
//    }
//
//    private Map<String, String> basicConnectorConfig() {
//        final Map<String, String> config = new HashMap<>();
//        config.put("name", CONNECTOR_NAME);
//        config.put("connector.class", GcsSinkConnector.class.getName());
//        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
//        config.put("key.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
//        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
//        config.put("value.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
//        config.put("tasks.max", "1");
//        if (gcsCredentialsPath != null) {
//            config.put("gcs.credentials.path", gcsCredentialsPath);
//        }
//        if (gcsCredentialsJson != null) {
//            config.put("gcs.credentials.json", gcsCredentialsJson);
//        }
//        if (useFakeGCS()) {
//            config.put("gcs.endpoint", gcsEndpoint);
//        }
//        config.put("gcs.bucket.name", testBucketName);
//        config.put("file.name.prefix", gcsPrefix);
//        config.put("topics", testTopic0 + "," + testTopic1);
//        return config;
//    }
//
////    protected String getAvroBlobName(final int partition, final int startOffset, final CompressionType compression) {
////        return super.getBaseBlobName(partition, startOffset) + ".avro" + compression.extension();
////    }
//

    /// /    protected String getAvroBlobName(final int partition, final int startOffset) {
    /// /        return super.getBaseBlobName(partition, startOffset) + ".avro";
    /// /    }

    public final class AvroSerDe {
        private final KafkaAvroSerializer serializer;

        AvroSerDe() {
            Map<String, String> serializerConfig = new HashMap<>();
            serializerConfig.put("schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
            serializer = new KafkaAvroSerializer();
            serializer.configure(serializerConfig, false);
        }

        private List<Record> sendRecords(final int recordCount, Duration timeLimit) {
            return sendRecords(recordCount, 4, timeLimit, AvroTestDataFixture::generateAvroRecord);
        }

        private List<Record> sendRecords(final int recordCount, final int partitionCount, Duration timeLimit, final Function<Integer, GenericRecord> generator) {
            final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
            final List<Record> result = new ArrayList<>();
            for (int cnt = 0; cnt < recordCount; cnt++) {
                int partition = cnt % partitionCount;
                    final String key = "key-" + cnt;
                    byte[] serializedValue = serializer.serialize(testTopic, generator.apply(cnt));
                    byte[] serializedKey = key.getBytes(StandardCharsets.UTF_8);
                    sendFutures.add(sendMessageAsync(testTopic, partition, serializedKey, serializedValue));
                    result.add(new Record(serializedKey, serializedValue, partition));
            }
            awaitFutures(sendFutures, timeLimit);
            return result;
        }

        private List<Record> extractRecords(byte[] blobBytes) throws IOException {
            List<Record> items = new ArrayList<>();
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    reader.forEach( genericRecord -> {
                        byte[] key = ((ByteBuffer)genericRecord.get("key")).array();
                        byte[] value = ((ByteBuffer)genericRecord.get("value")).array();
                        items.add(new Record(key, value, -1));
                    });
                }
            }
            return items;
        }
    }
}
