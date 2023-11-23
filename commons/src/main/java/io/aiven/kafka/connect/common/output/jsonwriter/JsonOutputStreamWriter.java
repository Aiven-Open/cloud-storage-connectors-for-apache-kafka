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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.OutputStreamWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

class JsonOutputStreamWriter implements OutputStreamWriter {
    private static final byte[] BATCH_START = "[\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_SEPARATOR = ",\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BATCH_END = "\n]".getBytes(StandardCharsets.UTF_8);

    private final Map<String, OutputFieldBuilder> fieldBuilders;
    private final boolean envelopeEnabled;
    private final ObjectMapper objectMapper;

    JsonOutputStreamWriter(final Map<String, OutputFieldBuilder> fieldBuilders, final boolean envelopeEnabled) {
        this.fieldBuilders = fieldBuilders;
        this.envelopeEnabled = envelopeEnabled;
        this.objectMapper = new ObjectMapper();
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
        outputStream.write(objectMapper.writeValueAsBytes(getFields(record)));
    }

    @Override
    public void stopWriting(final OutputStream outputStream) throws IOException {
        outputStream.write(BATCH_END);
    }

    private JsonNode getFields(final SinkRecord record) throws IOException {
        if (envelopeEnabled) {
            final ObjectNode root = JsonNodeFactory.instance.objectNode();
            final Set<Map.Entry<String, OutputFieldBuilder>> entries = fieldBuilders.entrySet();
            for (final Map.Entry<String, OutputFieldBuilder> entry : entries) {
                final JsonNode node = entry.getValue().build(record);
                root.set(entry.getKey(), node);
            }
            return root;
        } else {
            // envelope can be disabled only in case of single field
            return fieldBuilders.entrySet().iterator().next().getValue().build(record);
        }
    }
}
