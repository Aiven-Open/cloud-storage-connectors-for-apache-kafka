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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;


class TimestampBuilder implements OutputFieldBuilder {

    @Override
    public JsonNode build(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");

        if (record.timestamp() == null) {
            return null;
        }
        final Instant date = Instant.ofEpochMilli(record.timestamp());
        final DateTimeFormatter formatter =  DateTimeFormatter.ISO_INSTANT;
        final String timestampAsISO = formatter.format(date);

        return JsonNodeFactory.instance.textNode(timestampAsISO);
    }
}
