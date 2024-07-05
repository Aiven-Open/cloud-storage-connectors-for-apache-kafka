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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldType;

public class JsonOutputFieldComposer {
    public final Map<String, OutputFieldBuilder> fieldBuilders = new HashMap<>();

    public final JsonOutputFieldComposer addFields(final Collection<OutputField> fields) {
        Objects.requireNonNull(fields, "fields cannot be null");

        for (final OutputField field : fields) {
            fieldBuilders.put(field.getFieldType().name, resolveBuilderFor(field.getFieldType()));
        }

        return this;
    }

    private OutputFieldBuilder resolveBuilderFor(final OutputFieldType fieldType) {
        switch (fieldType) {
            case KEY :
                return new KeyBuilder();

            case VALUE :
                return new ValueBuilder();

            case OFFSET :
                return new OffsetBuilder();

            case TIMESTAMP :
                return new TimestampBuilder();

            case HEADERS :
                return new HeaderBuilder();

            default :
                throw new ConnectException("Unknown output field type " + fieldType);
        }
    }

}
