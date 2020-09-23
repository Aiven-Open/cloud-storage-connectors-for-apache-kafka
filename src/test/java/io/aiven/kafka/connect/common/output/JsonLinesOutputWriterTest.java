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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.jsonwriter.JsonLinesOutputWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class JsonLinesOutputWriterTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    protected ByteArrayOutputStream byteStream;
    protected JsonLinesOutputWriter sut;

    protected SinkRecord createRecord(final String key,
                                      final Schema valueSchema,
                                      final Object value,
                                      final int offset,
                                      final Long timestamp
    ) {
        return new SinkRecord(
                "anyTopic",
                0,
                Schema.STRING_SCHEMA,
                key,
                valueSchema,
                value,
                offset,
                timestamp,
                TimestampType.CREATE_TIME);
    }

    protected void assertRecords(final List<SinkRecord> records, final String expected) throws IOException {
        for (int i = 0; i < records.size() - 1; i++) {
            sut.writeRecord(records.get(i), byteStream);
        }
        sut.writeLastRecord(records.get(records.size() - 1), byteStream);
        assertEquals(expected, parseJsonLines(byteStream.toByteArray()));
    }

    // It also makes sure that bytes represents a valid JSONs lines with \n as Delimiter
    protected String parseJsonLines(final byte[] json) throws IOException {
        final Charset utf8 = StandardCharsets.UTF_8;
        final ByteArrayInputStream stream = new ByteArrayInputStream(json);
        final InputStreamReader streamReader = new InputStreamReader(stream, utf8);
        final BufferedReader bufferedReader = new BufferedReader(streamReader);
        final StringBuilder stringBuilder = new StringBuilder();

        String jsonLine = bufferedReader.readLine();
        while (jsonLine != null) {
            stringBuilder.append(objectMapper.readTree(jsonLine.getBytes(utf8)).toString());
            jsonLine = bufferedReader.readLine();
            if (jsonLine != null) {
                stringBuilder.append("\n");
            }
        }
        return stringBuilder.toString();
    }
}

