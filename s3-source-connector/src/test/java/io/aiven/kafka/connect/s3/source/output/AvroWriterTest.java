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

package io.aiven.kafka.connect.s3.source.output;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

final class AvroWriterTest {

    @Mock
    private S3SourceConfig s3SourceConfig;

    private AvroWriter avroWriter;
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        avroWriter = new AvroWriter();
        config = new HashMap<>();
    }

    @Test
    void testConfigureValueConverter() {
        final String value = "http://localhost:8081";
        when(s3SourceConfig.getString(SCHEMA_REGISTRY_URL)).thenReturn(value);
        avroWriter.configureValueConverter(config, s3SourceConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo("http://localhost:8081")
                .describedAs("The schema registry URL should be correctly set in the config.");
    }

    @Test
    void testReadAvroRecordsInvalidData() {
        final InputStream inputStream = new ByteArrayInputStream("mock-avro-data".getBytes(StandardCharsets.UTF_8));

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        final List<Object> records = avroWriter.readAvroRecords(inputStream, datumReader);

        assertThat(records.size()).isEqualTo(0);
    }

    @Test
    void testReadAvroRecords() throws Exception {
        final ByteArrayOutputStream avroData = generateMockAvroData();
        final InputStream inputStream = new ByteArrayInputStream(avroData.toByteArray());

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        final List<Object> records = avroWriter.readAvroRecords(inputStream, datumReader);

        assertThat(records.size()).isEqualTo(2);
    }

    ByteArrayOutputStream generateMockAvroData() throws IOException {
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        return getAvroRecord(schema, 2);
    }

    private static ByteArrayOutputStream getAvroRecord(final Schema schema, final int messageId) throws IOException {
        // Create Avro records
        final GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + messageId);
        avroRecord.put("id", messageId);

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(avroRecord); // record 1
            dataFileWriter.append(avroRecord); // record 2
            dataFileWriter.flush();
        }
        outputStream.close();
        return outputStream;
    }
}
