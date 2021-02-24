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
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonLinesOutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonOutputWriter;
import io.aiven.kafka.connect.common.output.plainwriter.PlainOutputWriter;

import com.github.luben.zstd.ZstdOutputStream;
import org.xerial.snappy.SnappyOutputStream;

public abstract class OutputWriter implements AutoCloseable {

    private final OutputStreamWriter writer;

    protected final OutputStream outputStream;

    private Boolean isOutputEmpty;

    private Boolean isClosed;

    protected OutputWriter(final OutputStream outputStream, final OutputStreamWriter writer) {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(outputStream, "outputStream");
        this.writer = writer;
        this.outputStream = outputStream;
        this.isOutputEmpty = true;
        this.isClosed = false;
    }

    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        for (final var r : sinkRecords) {
            writeRecord(r);
        }
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected CompressionType compressionType;

        protected Map<String, String> externalProperties;

        protected Collection<OutputField> outputFields;

        protected OutputStream outputStream;

        public Builder withCompressionType(final CompressionType compressionType) {
            if (Objects.isNull(compressionType)) {
                this.compressionType = CompressionType.NONE;
            }
            this.compressionType = compressionType;
            return this;
        }

        public Builder withExternalProperties(final Map<String, String> externalProperties) {
            this.externalProperties = externalProperties;
            return this;
        }

        public Builder withOutputFields(final Collection<OutputField> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public Builder withOutputStream(final OutputStream outputStream) {
            this.outputStream = outputStream;
            return this;
        }

        public OutputWriter build(final FormatType formatType) throws IOException {
            Objects.requireNonNull(outputFields, "Output fields haven't been set");
            Objects.requireNonNull(outputStream, "Output stream hasn't been set");
            switch (formatType) {
                case CSV:
                    return new PlainOutputWriter(outputFields, getCompressedStream(outputStream));
                case JSONL:
                    return new JsonLinesOutputWriter(outputFields, getCompressedStream(outputStream));
                case JSON:
                    return new JsonOutputWriter(outputFields, getCompressedStream(outputStream));
                default:
                    throw new ConnectException("Unsupported format type " + formatType);
            }
        }

        private OutputStream getCompressedStream(final OutputStream outputStream) throws IOException {
            switch (compressionType) {
                case ZSTD:
                    return new ZstdOutputStream(outputStream);
                case GZIP:
                    return new GZIPOutputStream(outputStream);
                case SNAPPY:
                    return new SnappyOutputStream(outputStream);
                default:
                    return outputStream;
            }
        }

    }

}
