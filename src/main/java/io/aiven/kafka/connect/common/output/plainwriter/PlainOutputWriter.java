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

package io.aiven.kafka.connect.common.output.plainwriter;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.OutputStreamWriter;
import io.aiven.kafka.connect.common.output.OutputWriter;


public class PlainOutputWriter extends OutputWriter {

    PlainOutputWriter(final OutputStream outputStream,
                      final OutputStreamWriter outputStreamWriter) {
        super(outputStream, outputStreamWriter);
    }

    public static PlainOutputWriter createFor(final Collection<OutputField> fields,
                                              final OutputStream outputStream) {
        final var outputStreamWriter = new Builder().addFields(fields).build();
        return new PlainOutputWriter(outputStream, outputStreamWriter);
    }

    static final class Builder {
        private final List<OutputFieldPlainWriter> writers = new ArrayList<>();

        final Builder addFields(final Collection<OutputField> fields) {
            Objects.requireNonNull(fields, "fields cannot be null");
            for (final OutputField field : fields) {
                switch (field.getFieldType()) {
                    case KEY:
                        writers.add(new KeyPlainWriter());
                        break;

                    case VALUE:
                        switch (field.getEncodingType()) {
                            case NONE:
                                writers.add(new ValuePlainWriter());
                                break;

                            case BASE64:
                                writers.add(new Base64ValuePlainWriter());
                                break;

                            default:
                                throw new ConnectException("Unknown output field encoding type "
                                    + field.getEncodingType());
                        }
                        break;

                    case OFFSET:
                        writers.add(new OffsetPlainWriter());
                        break;

                    case TIMESTAMP:
                        writers.add(new TimestampPlainWriter());
                        break;

                    case HEADERS:
                        writers.add(new HeadersPlainWriter());
                        break;

                    default:
                        throw new ConnectException("Unknown output field type " + field);
                }
            }

            return this;
        }

        final PlainOutputStreamWriter build() {
            return new PlainOutputStreamWriter(writers);
        }

    }
}
