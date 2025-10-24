package io.aiven.kafka.connect.common.integration.sink;


import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.format.AvroTestDataFixture;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSinkGenericRecordIntegrationTest<K extends Comparable<K>, N> extends AbstractSinkIntegrationBase<K, String, GenericRecord> {

    protected final FormatType formatType;

    protected AbstractSinkGenericRecordIntegrationTest(FormatType formatType) {
        this.formatType = Objects.requireNonNull(formatType);
    }

    /**
     * A record of the key, value and partition as written to kafka.
     * Equality is only checked against key and value.
     */
    public static class Record implements Comparable<Record> {
        private final byte[] key;
        private final byte[] value;
        private final int partition;
        private final GenericRecord record;

        /**
         * Constructor
         * @param key the key.
         * @param value the value
         * @param partition the partition
         */
        public Record(byte[] key, byte[] value, int partition) {
            Objects.requireNonNull(value, "value must not be null");
            this.key = key;
            this.value = value;
            this.partition = partition;
            this.record = null;
        }

        /**
         * Constructor
         * @param key the key.
         * @param value the value
         * @param genericRecord the partition
         */
        public Record(byte[] key, byte[] value, GenericRecord genericRecord) {
            Objects.requireNonNull(value, "value must not be null");
            this.key = key;
            this.value = value;
            this.partition = -1;
            this.record = genericRecord;
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
            int result = Arrays.compare(value, o.value);
            return result == 0 ? Arrays.compare(key, o.key) : result;
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
            return value.hashCode();
        }

        @Override
        public String toString() {
            return String.format("Record(key=%s, value=%s, partition=%d)", key == null ? key : new Utf8(key), new Utf8(value), partition);
        }
    }

    @Override
    protected final Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = super.basicConnectorConfig();
        config.put("format.output.type", formatType.name());
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        return config;
    }

    @Override
    protected final Map<String, Object> getProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        return props;
    }

//    @Test
//    void avroOutput() throws IOException {
//        final AvroSerDe serializer = new AvroSerDe();
//        CompressionType compressionType = CompressionType.NONE;
//        final Map<String, String> connectorConfig = basicConnectorConfig();
//        connectorConfig.put("format.output.fields", "key,value");
//        connectorConfig.put("file.compression.type", compressionType.name);
//        createConnector(connectorConfig);
//
//        List<Record> avroRecords = serializer.sendRecords(40, Duration.ofSeconds(45));
//
//        final Map<K, List<Record>> expectedBlobs = new HashMap<>();
//        avroRecords.forEach(record -> {
//            expectedBlobs.compute(getNativeKey(record.getPartition(), 0, CompressionType.NONE, FormatType.AVRO), (k, v) -> v == null ? new ArrayList<>() : v)
//                    .add(record);
//        });
//
//        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));
//
//        for (final K nativeKey : expectedBlobs.keySet()) {
//            final List<Record> items = serializer.extractRecords(bucketAccessor.readBytes(nativeKey));
//            assertThat(items).containsExactlyInAnyOrderElementsOf(expectedBlobs.get(nativeKey));
//        }
//    }
//
//    @Test
//    void schemaChanged() throws IOException {
//        final AvroSerDe serializer = new AvroSerDe();
//        final Map<String, String> connectorConfig = basicConnectorConfig();
//        connectorConfig.put("format.output.envelope", "false");
//        connectorConfig.put("format.output.fields", "value");
//        connectorConfig.put("format.output.fields.value.encoding", "none");
//        createConnector(connectorConfig);
//
//        // write one record with old schema, one with new, and one with old again.  All written into partition 0
//        List<Record> avroRecords = serializer.sendRecords(3, 1, Duration.ofSeconds(3), (i) -> {
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
//
//        // since each record changes the schema each record should be in its own partition.
//        final List<K> expectedKeys = new ArrayList<>();
//        for (int i = 0; i < 3; i++)
//        {
//            expectedKeys.add(getNativeKey(0, i, CompressionType.NONE, FormatType.AVRO));
//        }
//
//        awaitAllBlobsWritten(expectedKeys, Duration.ofSeconds(45));
//
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
//    }
//
//    /**
//     * Methods to serialize and deserialize records from the sink storage.
//     */
//    public final class AvroSerDe {
//
//        KafkaAvroSerializer serializer;
//
//        AvroSerDe() {
//            Map<String, String> serializerConfig = new HashMap<>();
//            serializerConfig.put("schema.registry.url", getKafkaManager().getSchemaRegistryUrl());
//            serializer = new KafkaAvroSerializer();
//            serializer.configure(serializerConfig, false);
//        }
//
//        /**
//         * Creates and sends records with the default testing schema.
//         * Will wait for the records to be sent before returning.
//         * @param recordCount the number of records to send.
//         * @param timeLimit the time limit to wait for the records to be sent.
//         * @return a list of {@link Record} representing each record sent.
//         */
//        private List<Record> sendRecords(final int recordCount, Duration timeLimit) {
//            return sendRecords(recordCount, 4, timeLimit, AvroTestDataFixture::generateAvroRecord);
//        }
//
//        /**
//         * Creates and sends records generated with the specified generator.
//         * Will wait for the records to be sent before returning.
//         * @param recordCount the number of records to send.
//         * @param partitionCount the number of partitions to split the records across.
//         * @param timeLimit the time limit to wait for the records to be sent.
//         * @param generator The function to convert the current record number to a GenericRecord to send.
//         * @return a list of {@link Record} representing each record sent.
//         */
//        private List<Record> sendRecords(final int recordCount, final int partitionCount, Duration timeLimit, final Function<Integer, GenericRecord> generator) {
//            final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
//            final List<Record> result = new ArrayList<>();
//            for (int cnt = 0; cnt < recordCount; cnt++) {
//                int partition = cnt % partitionCount;
//                    final String key = "key-" + cnt;
//                    GenericRecord value = generator.apply(cnt);
//                    byte[] serializedValue = serializer.serialize(testTopic, value);
//                    byte[] serializedKey = key.getBytes(StandardCharsets.UTF_8);
//                    sendFutures.add(sendMessageAsync(testTopic, partition, key, value));
//                    result.add(new Record(serializedKey, serializedValue, partition));
//            }
//            awaitFutures(sendFutures, timeLimit);
//            return result;
//        }
//
//        /**
//         * Extract {@link Record} values from a blob of data read from the storage.
//         * @param blobBytes the bytes for the blob of data.
//         * @return a list of {@link Record} representing each record read from the blob.
//         * @throws IOException on error reading blob.
//         */
//        private List<Record> extractRecords(byte[] blobBytes) throws IOException {
//            List<Record> items = new ArrayList<>();
//            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
//                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
//                    reader.forEach( genericRecord -> {
//                        byte[] key = genericRecord.hasField("key") ? ((ByteBuffer)genericRecord.get("key")).array() : null;
//                        byte[] value = ((ByteBuffer)genericRecord.get("value")).array();
//                        items.add(new Record(key, value, -1));
//                    });
//                }
//            }
//            return items;
//        }
//
//        /**
//         * Extract {@link Record} values from a blob of data read from the storage.
//         * @param blobBytes the bytes for the blob of data.
//         * @return a list of {@link Record} representing each record read from the blob.
//         * @throws IOException on error reading blob.
//         */
//        private List<GenericRecord> extractGenericRecord(byte[] blobBytes) throws IOException {
//            List<GenericRecord> items = new ArrayList<>();
//            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
//                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
//                    reader.forEach(items::add);
//                }
//            }
//            return items;
//        }
//    }
}
