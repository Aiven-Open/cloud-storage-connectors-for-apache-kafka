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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class KeyBuilder implements OutputFieldBuilder {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final JsonConverter converter;

    public KeyBuilder() {
        converter = new JsonConverter();
        // TODO: make a more generic configuration
        converter.configure(Map.of("schemas.enable", false, "converter.type", "key"));
    }

    /**
     * Takes the {@link SinkRecord}'s key as a JSON.
     *
     * <p>
     * If the key is {@code null}, it outputs nothing.
     *
     * <p>
     * If the key is not {@code null}, it assumes the key <b>is</b> a JSON
     *
     * @param record
     *            the record to get the key from
     * @throws DataException
     *             when the key is not convertible to Json
     */
    @Override
    public JsonNode build(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        if (record.key() == null) {
            return null;
        }

        return objectMapper.readTree(converter.fromConnectData(record.topic(), record.keySchema(), record.key()));
    }
}
