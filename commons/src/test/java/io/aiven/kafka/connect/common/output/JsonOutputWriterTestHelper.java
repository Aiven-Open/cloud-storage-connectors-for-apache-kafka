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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class JsonOutputWriterTestHelper {
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final Schema level1Schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
    protected ByteArrayOutputStream byteStream;
    protected OutputWriter sut;

    protected SinkRecord createRecord(final String key, final Schema valueSchema, final Object value, final int offset,
            final Long timestamp) {
        return new SinkRecord("anyTopic", 0, Schema.STRING_SCHEMA, key, valueSchema, value, offset, timestamp,
                TimestampType.CREATE_TIME);
    }

    protected String useWithWrongLastRecord(final List<SinkRecord> records) throws IOException {
        for (final SinkRecord record : records) {
            sut.writeRecord(record);
        }
        final byte[] result = byteStream.toByteArray();
        return parseJson(result);
    }

    // It also makes sure that bytes represents a valid JSON
    protected void assertRecords(final List<SinkRecord> records, final String expected) throws IOException {

        for (final SinkRecord record : records) {
            sut.writeRecord(record);
        }
        sut.close();
        assertThat(parseJson(byteStream.toByteArray())).isEqualTo(expected);
    }

    abstract String parseJson(byte[] json) throws IOException;
}
