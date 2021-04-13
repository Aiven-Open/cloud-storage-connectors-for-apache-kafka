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

package io.aiven.kafka.connect.common.output.jsonwriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.aiven.kafka.connect.common.output.OutputStreamWriter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

class PlainValueJsonOutputStreamWriter implements OutputStreamWriter {

    private final ObjectMapper objectMapper;
    private final OutputFieldBuilder valueBuilder;
    private static final byte[] BATCH_START = "[\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_SEPARATOR = ",\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BATCH_END = "\n]".getBytes(StandardCharsets.UTF_8);

    PlainValueJsonOutputStreamWriter() {
        this.objectMapper = new ObjectMapper();
        this.valueBuilder = new ValueBuilder();
        objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    @Override
    public void startWriting(final OutputStream outputStream) throws IOException {
        outputStream.write(BATCH_START);
    }

    @Override
    public void writeRecordsSeparator(final OutputStream outputStream) throws IOException {
        outputStream.write(RECORD_SEPARATOR);
    }

    @Override
    public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        outputStream.write(objectMapper.writeValueAsBytes(valueBuilder.build(record)));
    }

    @Override
    public void stopWriting(final OutputStream outputStream) throws IOException {
        outputStream.write(BATCH_END);
    }
}
