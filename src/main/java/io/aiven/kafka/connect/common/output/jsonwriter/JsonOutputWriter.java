/*
 * Copyright 2021 Aiven Oy
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

import java.io.OutputStream;
import java.util.Collection;
import java.util.Objects;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.OutputWriter;


public class JsonOutputWriter extends OutputWriter {

    public JsonOutputWriter(final Collection<OutputField> fields, final OutputStream outputStream) {
        super(outputStream, new Builder().addFields(fields).build());
    }

    static final class Builder {
        private final JsonOutputFieldComposer fieldsComposer = new JsonOutputFieldComposer();

        final JsonOutputWriter.Builder addFields(final Collection<OutputField> fields) {
            Objects.requireNonNull(fields, "fields cannot be null");

            fieldsComposer.addFields(fields);
            return this;
        }

        final JsonOutputStreamWriter build() {
            return new JsonOutputStreamWriter(fieldsComposer.fieldBuilders);
        }
    }
}
