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

package io.aiven.kafka.connect.common.integration.sink;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.format.AvroTestDataFixture;
import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSinkParquetIntegrationTest<K extends Comparable<K>, N> extends AbstractSinkGenericRecordIntegrationTest<K, N> {

    public AbstractSinkParquetIntegrationTest() {
        super(FormatType.PARQUET);
    }

    @Test
    void allOutputFields(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        CompressionType compressionType = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        int recordCount = 40;
        int partitionCount = 4;
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<Integer>> expectedBlobs = new HashMap<>();
        for (int cnt = 0; cnt < recordCount; cnt++) {
            int partition = cnt % partitionCount;
            final String key = "key-" + cnt;
            GenericRecord value = AvroTestDataFixture.generateAvroRecord(cnt);
            sendFutures.add(sendMessageAsync(testTopic, partition, key, value));
            expectedBlobs.compute(getNativeKey(partition, 0, CompressionType.NONE, formatType), (k, v) -> v == null ? new ArrayList<>() : v)
                    .add(cnt);
        }

        awaitFutures(sendFutures, Duration.ofSeconds(45));

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));

        for (final K nativeKey : expectedBlobs.keySet()) {
            List<GenericRecord> records = ParquetTestDataFixture.readRecords(tmpDir, bucketAccessor.readBytes(nativeKey));
            List<Integer> recordNumbers = expectedBlobs.get(nativeKey);
            assertThat(records).hasSize(recordNumbers.size());
            for (int i = 0; i < recordNumbers.size(); i++) {
                int recordNumber = recordNumbers.get(i);
                GenericRecord record = records.get(i);
                assertThat(record.get("key")).hasToString("key-" + recordNumber);
                assertThat(record.get("value")).isNotNull();
                assertThat(record.get("offset")).isEqualTo((long) i);
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();
                GenericRecord valueRecord = (GenericRecord) record.get("value");
                assertThat(valueRecord.get("id")).isEqualTo(recordNumber);
                assertThat(valueRecord.get("message")).hasToString("Hello, from Avro Test Data Fixture! object " + recordNumber);
            }
        }
    }


    @Test
    void schemaChanged(@TempDir final Path tmpDir) throws ExecutionException, InterruptedException, IOException {
        CompressionType compressionType = CompressionType.NONE;
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compressionType.name);
        createConnector(connectorConfig);

        int recordCount = 3;
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<K, List<Integer>> expectedBlobs = new HashMap<>();
        for (int cnt = 0; cnt < recordCount; cnt++) {
            boolean newSchema = (cnt % 2) == 1;
            final String key = "key-" + cnt;
            GenericRecord value =  new GenericData.Record(newSchema ? AvroTestDataFixture.EVOLVED_SCHEMA : AvroTestDataFixture.DEFAULT_SCHEMA);
            value.put("message", new Utf8("user-" + cnt));
            value.put("id", cnt);
            if (newSchema) {
                value.put("age", cnt * 10);
            }
            sendFutures.add(sendMessageAsync(testTopic, 0, key, value));
            expectedBlobs.compute(getNativeKey(0, cnt, CompressionType.NONE, formatType), (k, v) -> v == null ? new ArrayList<>() : v)
                    .add(cnt);
        }

        awaitFutures(sendFutures, Duration.ofSeconds(45));

        awaitAllBlobsWritten(expectedBlobs.keySet(), Duration.ofSeconds(45));

        for (final K nativeKey : expectedBlobs.keySet()) {
            List<GenericRecord> records = ParquetTestDataFixture.readRecords(tmpDir, bucketAccessor.readBytes(nativeKey));
            List<Integer> recordNumbers = expectedBlobs.get(nativeKey);
            assertThat(records).hasSize(recordNumbers.size());
            for (int i = 0; i < recordNumbers.size(); i++) {
                int recordNumber = recordNumbers.get(i);
                GenericRecord record = records.get(i);
                assertThat(record.get("value")).isNotNull();
                GenericRecord valueRecord = (GenericRecord) record.get("value");
                assertThat(valueRecord.get("id")).isEqualTo(recordNumber);
                assertThat(valueRecord.get("message")).hasToString("user-" + recordNumber);
                boolean newSchema = (recordNumber % 2) == 1;
                if (newSchema) {
                    assertThat(valueRecord.get("age")).isEqualTo(recordNumber * 10);
                } else {
                    assertThat(valueRecord.hasField("age")).isFalse();
                }
            }
        }
    }
}
