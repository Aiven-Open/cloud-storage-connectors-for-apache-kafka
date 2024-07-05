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
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class HeaderBuilder implements OutputFieldBuilder {

    private final JsonConverter converter;

    public HeaderBuilder() {
        converter = new JsonConverter();
        // TODO: make a more generic configuration
        converter.configure(Map.of("schemas.enable", false, "converter.type", "header"));
    }

    @Override
    public JsonNode build(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");

        if (record.headers() == null) {
            return null;
        }

        final ArrayNode root = JsonNodeFactory.instance.arrayNode();

        final String topic = record.topic();
        for (final Header header : record.headers()) {
            final ObjectNode headerRoot = JsonNodeFactory.instance.objectNode();
            final String key = header.key();
            headerRoot.put("key", key);
            final JsonNode headerNode = nodeFromHeader(header, topic);
            headerRoot.set("value", headerNode);
            root.add(headerRoot);
        }
        return root;
    }

    private JsonNode nodeFromHeader(final Header header, final String topic) throws IOException {
        return ObjectMapperProvider.get()
                .readTree(converter.fromConnectHeader(topic, header.key(), header.schema(), header.value()));
    }
}
