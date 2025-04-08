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

package io.aiven.kafka.connect.common.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonOutputWriter;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class JsonOutputWriterTest extends JsonOutputWriterTestHelper {
    private final static OutputWriter NONE = null;
    private final OutputFieldEncodingType noEncoding = OutputFieldEncodingType.NONE;

    @BeforeEach
    void setUp() {
        byteStream = new ByteArrayOutputStream();
        sut = NONE;
    }

    @Test
    void jsonValueWithoutMetadataAndValue() throws IOException {
        sut = new JsonOutputWriter(Collections.emptyList(), byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);

        final String expected = "[{}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithValue() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);

        final String expected = "[{\"value\":{\"name\":\"John\"}}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithSingleField() throws IOException {
        final List<OutputField> fields = Collections.singletonList(new OutputField(OutputFieldType.VALUE, noEncoding));
        final Struct struct = new Struct(level1Schema).put("name", "John");
        final SinkRecord record = createRecord("key0", level1Schema, struct, 1, 1000L);

        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            try (JsonOutputWriter writer = new JsonOutputWriter(fields, byteStream, false)) {
                writer.writeRecord(record);
            }
            assertThat(byteStream).hasToString("[\n{\"name\":\"John\"}\n]");
        }
    }

    @Test
    void jsonValueWithOneFieldAndValue() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.KEY, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);

        final String expected = "[{\"value\":{\"name\":\"John\"},\"key\":\"key0\"}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void multiStringJsonValueWithOneFieldAndValue() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.KEY, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");
        final Struct struct2 = new Struct(level1Schema).put("name", "Pekka");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);
        final SinkRecord record2 = createRecord("key0", level1Schema, struct2, 1, 1000L);

        final String expected = "[{\"value\":{\"name\":\"John\"},\"key\":\"key0\"},"
                + "{\"value\":{\"name\":\"Pekka\"},\"key\":\"key0\"}]";

        assertRecords(List.of(record1, record2), expected);
    }

    @Test
    void jsonValueWithAllMetadata() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.KEY, noEncoding), new OutputField(OutputFieldType.OFFSET, noEncoding),
                new OutputField(OutputFieldType.TIMESTAMP, noEncoding),
                new OutputField(OutputFieldType.HEADERS, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);
        record1.headers().add("headerKey", "headerValue", Schema.STRING_SCHEMA);

        final String expected = "[{\"headers\":[{\"key\":\"headerKey\",\"value\":\"headerValue\"}]," + "\"offset\":1,"
                + "\"value\":{\"name\":\"John\"}," + "\"key\":\"key0\"," + "\"timestamp\":\"1970-01-01T00:00:01Z\"}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithMultipleHeaders() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.HEADERS, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);
        record1.headers().add("headerKey1", "headerValue1", Schema.STRING_SCHEMA);
        record1.headers().add("headerKey2", "headerValue2", Schema.STRING_SCHEMA);

        final String expected = "[{\"headers\":" + "[{\"key\":\"headerKey1\",\"value\":\"headerValue1\"},"
                + "{\"key\":\"headerKey2\",\"value\":\"headerValue2\"}]," + "\"value\":{\"name\":\"John\"}}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithMissingValue() throws IOException {
        final List<OutputField> fields = Collections.singletonList(new OutputField(OutputFieldType.VALUE, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);

        final SinkRecord record1 = createRecord("key0", level1Schema, null, 1, 1000L);

        final String expected = "[{\"value\":null}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithMissingKey() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.KEY, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Schema level1Schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord(null, level1Schema, struct1, 1, 1000L);

        final String expected = "[{\"value\":{\"name\":\"John\"},\"key\":null}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithMissingTimestamp() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.TIMESTAMP, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Schema level1Schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord(null, level1Schema, struct1, 1, null);

        final String expected = "[{\"value\":{\"name\":\"John\"},\"timestamp\":null}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void jsonValueWithMissingHeader() throws IOException {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding),
                new OutputField(OutputFieldType.HEADERS, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);
        final Struct struct1 = new Struct(level1Schema).put("name", "John");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);

        final String expected = "[{\"headers\":[],\"value\":{\"name\":\"John\"}}]";

        assertRecords(Collections.singletonList(record1), expected);
    }

    @Test
    void failedIfLastRecordIsMissing() {
        final List<OutputField> fields = List.of(new OutputField(OutputFieldType.VALUE, noEncoding));
        sut = new JsonOutputWriter(fields, byteStream);

        final Struct struct1 = new Struct(level1Schema).put("name", "John");
        final Struct struct2 = new Struct(level1Schema).put("name", "Pekka");

        final SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);
        final SinkRecord record2 = createRecord("key0", level1Schema, struct2, 1, 1000L);

        assertThatThrownBy(() -> useWithWrongLastRecord(List.of(record1, record2)))
                .isInstanceOf(JsonParseException.class);
    }

    @Override
    String parseJson(final byte[] json) throws IOException {
        return objectMapper.readTree(json).toString();
    }
}
