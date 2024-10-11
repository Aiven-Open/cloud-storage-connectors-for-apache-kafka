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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.*;
import static io.aiven.kafka.connect.s3.source.output.TransformerFactory.DEFAULT_TRANSFORMER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.AivenKafkaConnectS3SourceConnector;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

final class AvroTransformerTest {

    @Mock
    private S3SourceConfig s3SourceConfig;

    private AvroTransformer underTest;
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        underTest = new AvroTransformer();
        config = new HashMap<>();
        when(s3SourceConfig.getClass(ArgumentMatchers.eq(VALUE_SERIALIZER))).thenReturn((Class)KafkaAvroSerializer.class);
        when(s3SourceConfig.getString(ArgumentMatchers.eq(SCHEMA_REGISTRY_URL))).thenReturn("file:invalid");
    }

    @Test
    public void testName() {
        assertThat(underTest.getName().equals("avro"));
    }

    @Test
    void testConfigureValueConverter() {
        final String value = "http://localhost:8081";
        when(s3SourceConfig.getString(SCHEMA_REGISTRY_URL)).thenReturn(value).thenReturn(null).thenReturn("");

        // test value
        underTest.configureValueConverter(config, s3SourceConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo("http://localhost:8081")
                .describedAs("The schema registry URL should be correctly set in the config.");

        // test null
        underTest.configureValueConverter(config, s3SourceConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo(null)
                .describedAs("The schema registry URL should be null");

        // test empty String
        underTest.configureValueConverter(config, s3SourceConfig);
        assertThat(config.get(SCHEMA_REGISTRY_URL)).isEqualTo("")
                .describedAs("The schema registry URL should be an empty string");

    }

    @Test
    void testByteArrayIteratorInvalidData() {
        final InputStream inputStream = new ByteArrayInputStream("mock-avro-data".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> underTest.byteArrayIterator(inputStream, "topic", s3SourceConfig))
                .isInstanceOf(BadDataException.class);
    }

    @Test
    void testReadAvroRecords() throws Exception {
        final ByteArrayOutputStream avroData = generateMockAvroData();
        final InputStream inputStream = new ByteArrayInputStream(avroData.toByteArray());

        final Iterator<byte[]> records = underTest.byteArrayIterator(inputStream, "topic", s3SourceConfig);
        int count = 0;
        while (records.hasNext()) {
            count++;
        }
        assertThat(count).isEqualTo(2);
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

//    private void setBasicProperties() {
//        properties.put(S3SourceConfig.OUTPUT_FORMAT_KEY, "avro");
//        properties.put("name", "test_source_connector");
//        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
//        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
//        properties.put("tasks.max", "1");
//        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
//        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
//        properties.put(TARGET_TOPICS, "testtopic");
//    }
}
