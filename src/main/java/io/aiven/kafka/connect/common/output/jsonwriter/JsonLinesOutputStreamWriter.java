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
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.OutputStreamWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


class JsonLinesOutputStreamWriter implements OutputStreamWriter {

    private final Map<String, OutputFieldBuilder> fieldBuilders;
    private static final byte[] RECORD_SEPARATOR = "\n".getBytes(StandardCharsets.UTF_8);
    private final ObjectMapper objectMapper;

    JsonLinesOutputStreamWriter(final Map<String, OutputFieldBuilder> fieldBuilders) {
        this.fieldBuilders = fieldBuilders;
        this.objectMapper = new ObjectMapper();
        objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    @Override
    public void writeRecordsSeparator(final OutputStream outputStream) throws IOException {
        outputStream.write(RECORD_SEPARATOR);
    }

    @Override
    public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        outputStream.write(objectMapper.writeValueAsBytes(getFields(record)));
    }

    private ObjectNode getFields(final SinkRecord record) throws IOException {
        final Iterator<Map.Entry<String, OutputFieldBuilder>> writerIter = fieldBuilders.entrySet().iterator();


        final ObjectNode root = JsonNodeFactory.instance.objectNode();
        while (writerIter.hasNext()) {
            writeEntry(writerIter.next(), record, root);
        }
        return root;
    }

    private void writeEntry(final Map.Entry<String, OutputFieldBuilder> entry,
                            final SinkRecord record,
                            final ObjectNode root) throws IOException {
        final JsonNode node = entry.getValue().build(record);
        root.set(entry.getKey(), node);
    }
}
