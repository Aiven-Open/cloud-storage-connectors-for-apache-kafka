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

package io.aiven.kafka.connect.common.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class OutputWriter implements AutoCloseable {

    private final OutputStreamWriter writer;

    protected final OutputStream outputStream;

    private Boolean isOutputEmpty;

    private Boolean isClosed;

    protected final Map<String, String> externalConfiguration;

    protected OutputWriter(final OutputStream outputStream, final OutputStreamWriter writer) {
        this(outputStream, writer, Collections.emptyMap());
    }

    protected OutputWriter(final OutputStream outputStream, final OutputStreamWriter writer,
            final Map<String, String> externalConfiguration) {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(outputStream, "outputStream");
        this.writer = writer;
        this.outputStream = outputStream;
        this.externalConfiguration = externalConfiguration;
        this.isOutputEmpty = true;
        this.isClosed = false;
    }

    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        Objects.requireNonNull(sinkRecords, "sinkRecords");
        if (sinkRecords.isEmpty()) {
            return;
        }
        for (final var record : sinkRecords) {
            writeRecord(record);
        }
    }

    public void writeRecord(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        if (this.isOutputEmpty) {
            writer.startWriting(outputStream);
            this.isOutputEmpty = false;
        } else {
            writer.writeRecordsSeparator(outputStream);
        }
        writer.writeOneRecord(outputStream, record);
    }

    @Override
    public void close() throws IOException {
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected CompressionType compressionType;

        protected Map<String, String> externalProperties;

        protected Collection<OutputField> outputFields;

        protected boolean envelopeEnabled = true;

        public Builder withCompressionType(final CompressionType compressionType) {
            if (Objects.isNull(compressionType)) {
                this.compressionType = CompressionType.NONE;
            }
            this.compressionType = compressionType;
            return this;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable externalProperties")
        public Builder withExternalProperties(final Map<String, String> externalProperties) {
            this.externalProperties = externalProperties;
            return this;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable outputFields")
        public Builder withOutputFields(final Collection<OutputField> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public Builder withEnvelopeEnabled(final Boolean enabled) {
            this.envelopeEnabled = enabled;
            return this;
        }

        public OutputWriter build(final OutputStream out, final FormatType formatType) throws IOException {
            Objects.requireNonNull(outputFields, "Output fields haven't been set");
            Objects.requireNonNull(out, "Output stream hasn't been set");
            if (Objects.requireNonNull(formatType) == FormatType.PARQUET) {
                /*
                 * parquet has its own way for compression, CompressionType passes by "file. compression. type"
                 * parameter in externalProperties to writer and set explicitly to AvroParquetWriter
                 */
                return formatType.getOutputWriter(out, outputFields, externalProperties, envelopeEnabled);
            }
            return formatType.getOutputWriter(compressionType.compress(out), outputFields, externalProperties,
                    envelopeEnabled);
        }
    }

}
