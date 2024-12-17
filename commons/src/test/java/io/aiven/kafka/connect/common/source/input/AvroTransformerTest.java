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

package io.aiven.kafka.connect.common.source.input;

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMA_REGISTRY_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class AvroTransformerTest {

    @Mock
    private SourceCommonConfig sourceCommonConfig;

    private AvroTransformer avroTransformer;
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        avroTransformer = new AvroTransformer(new AvroData(100));
        config = new HashMap<>();
    }

    @Test
    void testConfigureValueConverter() {
        final String value = "http://localhost:8081";
        when(sourceCommonConfig.getString(SCHEMA_REGISTRY_URL)).thenReturn(value);
        avroTransformer.configureValueConverter(config, sourceCommonConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo("http://localhost:8081")
                .describedAs("The schema registry URL should be correctly set in the config.");
    }

    @Test
    void testReadAvroRecordsInvalidData() {
        final InputStream inputStream = new ByteArrayInputStream("mock-avro-data".getBytes(StandardCharsets.UTF_8));

        final Stream<GenericRecord> records = avroTransformer.getRecords(() -> inputStream, "", 0, sourceCommonConfig,
                0);

        final List<Object> recs = records.collect(Collectors.toList());
        assertThat(recs).isEmpty();
    }

    @Test
    void testReadAvroRecords() throws Exception {
        final ByteArrayOutputStream avroData = generateMockAvroData(25);
        final InputStream inputStream = new ByteArrayInputStream(avroData.toByteArray());

        final Stream<GenericRecord> records = avroTransformer.getRecords(() -> inputStream, "", 0, sourceCommonConfig,
                0);

        final List<Object> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(25);
    }

    @Test
    void testReadAvroRecordsSkipFew() throws Exception {
        final ByteArrayOutputStream avroData = generateMockAvroData(20);
        final InputStream inputStream = new ByteArrayInputStream(avroData.toByteArray());

        final Stream<GenericRecord> records = avroTransformer.getRecords(() -> inputStream, "", 0, sourceCommonConfig,
                5);

        final List<Object> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(15);
        // get first rec
        assertThat(((GenericRecord) recs.get(0)).get("message").toString())
                .isEqualTo("Hello, Kafka Connect S3 Source! object 5");
    }

    @Test
    void testReadAvroRecordsSkipMoreRecordsThanExist() throws Exception {
        final ByteArrayOutputStream avroData = generateMockAvroData(20);
        final InputStream inputStream = new ByteArrayInputStream(avroData.toByteArray());

        final Stream<GenericRecord> records = avroTransformer.getRecords(() -> inputStream, "", 0, sourceCommonConfig,
                25);

        final List<Object> recs = records.collect(Collectors.toList());
        assertThat(recs).hasSize(0);
    }

    static ByteArrayOutputStream generateMockAvroData(final int numRecs) throws IOException {
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        return getAvroRecords(schema, numRecs);
    }

    private static ByteArrayOutputStream getAvroRecords(final Schema schema, final int numOfRecs) throws IOException {
        // Create Avro records
        final List<GenericRecord> avroRecords = new ArrayList<>();
        for (int i = 0; i < numOfRecs; i++) {
            final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD AvoidInstantiatingObjectsInLoops
            avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + i);
            avroRecord.put("id", i);
            avroRecords.add(avroRecord);
        }

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        // Append each record using a loop
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            for (final GenericRecord record : avroRecords) {
                dataFileWriter.append(record);
            }
            dataFileWriter.flush();
        }
        outputStream.close();
        return outputStream;
    }
}
