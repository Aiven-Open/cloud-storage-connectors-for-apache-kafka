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

package io.aiven.kafka.connect.common.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;

public abstract class OutputWriter implements AutoCloseable {

    private OutputStream outputStream;
    private Boolean isOutputEmpty;
    private Boolean isClosed;
    private OutputStreamWriter writer;

    public OutputWriter(final Collection<OutputField> fields,
                        final OutputStream outputStream) {
        this.writer = writer(fields);
        this.outputStream = outputStream;
        this.isOutputEmpty = true;
        this.isClosed = false;
    }

    public void writeRecord(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        if (!this.isOutputEmpty) {
            writer.writeRecordsSeparator(outputStream);
        } else {
            writer.startWriting(outputStream);
            this.isOutputEmpty = false;
        }
        writer.writeOneRecord(outputStream, record);
    }

    public void close()  throws IOException {
        if (!isClosed) {
            try {
                writer.stopWriting(outputStream);
                this.outputStream.flush();
            } finally {
                if (this.outputStream != null) {
                    this.outputStream.close();
                    this.isClosed = true;
                }
            }
        }
    }

    protected abstract OutputStreamWriter writer(final Collection<OutputField> fields);
}
