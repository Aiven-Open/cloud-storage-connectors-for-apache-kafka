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

package io.aiven.kafka.connect.common.output.plainwriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.OutputWriter;

public final class OutputPlainWriter implements OutputWriter {

    private static final byte[] FIELD_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_SEPARATOR = "\n".getBytes(StandardCharsets.UTF_8);

    private final List<OutputFieldPlainWriter> writers;

    private OutputPlainWriter(final List<OutputFieldPlainWriter> writers) {
        this.writers = writers;
    }

    public void writeRecord(final SinkRecord record,
                            final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        writeFields(record, outputStream);
        outputStream.write(RECORD_SEPARATOR);
    }

    public void writeLastRecord(final SinkRecord record,
                                final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        writeFields(record, outputStream);
    }

    private void writeFields(final SinkRecord record,
                             final OutputStream outputStream) throws IOException {
        final Iterator<OutputFieldPlainWriter> writerIter = writers.iterator();
        writerIter.next().write(record, outputStream);
        while (writerIter.hasNext()) {
            outputStream.write(FIELD_SEPARATOR);
            writerIter.next().write(record, outputStream);
        }
    }

    public static final class Builder {
        private final List<OutputFieldPlainWriter> writers = new ArrayList<>();

        public final Builder addFields(final Collection<OutputField> fields) {
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

        public final OutputPlainWriter build() {
            return new OutputPlainWriter(writers);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
