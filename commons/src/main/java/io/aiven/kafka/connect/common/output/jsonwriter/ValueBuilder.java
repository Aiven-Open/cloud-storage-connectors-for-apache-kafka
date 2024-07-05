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

class ValueBuilder implements OutputFieldBuilder {

    private final JsonConverter converter;

    public ValueBuilder() {
        converter = new JsonConverter();
        // TODO: make a more generic configuration
        converter.configure(Map.of("schemas.enable", false, "converter.type", "value"));
    }

    /**
     * Takes the {@link SinkRecord}'s value as a JSON.
     *
     * @param record
     *            the record to get the value from
     * @return JsonNode Value transformed to any JSON value
     * @throws DataException
     *             when the value is not actually a JSON
     */
    @Override
    public JsonNode build(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");

        if (record.value() == null) {
            return null;
        }

        return ObjectMapperProvider.get()
                .readTree(converter.fromConnectData(record.topic(), record.valueSchema(), record.value()));
    }
}
