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

package io.aiven.kafka.connect.common.config;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.output.avro.AvroOutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonLinesOutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonOutputWriter;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;
import io.aiven.kafka.connect.common.output.plainwriter.PlainOutputWriter;

public enum FormatType {
    /** Handles in Avro format */
    AVRO("avro", (stream, fields, config, envelope) -> new AvroOutputWriter(fields, stream, config, envelope)),
    /** Handles in CSV format */
    CSV("csv", (stream, fields, config, envelope) -> new PlainOutputWriter(fields, stream)),
    /** Handles in JSON format */
    JSON("json", (stream, fields, config, envelope) -> new JsonOutputWriter(fields, stream, envelope)),
    /** Handles in JSONL format */
    JSONL("jsonl", (stream, fields, config, envelope) -> new JsonLinesOutputWriter(fields, stream, envelope)),
    /** Handles Parquet format */
    PARQUET("parquet", (stream, fields, config, envelope) -> new ParquetOutputWriter(fields, stream, config, envelope));

    /**
     * A list of supported format types for display.
     */
    public static final String SUPPORTED_FORMAT_TYPES = FormatType.names()
            .stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    /**
     * The name of this type.
     */
    public final String name;

    /** The writer constructor for this type */
    private final WriterConstructor writerConstructor;

    /**
     * Constructor.
     *
     * @param name
     *            The name of this type.
     * @param writerConstructor
     *            the writer constructor for this type.
     */
    FormatType(final String name, final WriterConstructor writerConstructor) {
        this.name = name;
        this.writerConstructor = writerConstructor;
    }

    /**
     * Gets the format type by name.
     *
     * @param name
     *            the name to lookup.
     * @return the FormatType
     * @throws IllegalArgumentException
     *             if not found
     */
    public static FormatType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        try {
            return FormatType.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("Unknown format type: " + name);
        }
    }

    /**
     * Gets the collection of names.
     *
     * @return the collection of names.
     */
    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    /**
     * Gets the output writer for this format type.
     *
     * @param outputStream
     *            the stream to write the format to.
     * @param fields
     *            the OutputFields to output to the stream.
     * @param externalConfig
     *            an optional configuration for the writer.
     * @param envelopeEnabled
     *            true if the envelope is enabled.
     * @return the OutputWriter for this format type.
     */
    public OutputWriter getOutputWriter(final OutputStream outputStream, final Collection<OutputField> fields,
            final Map<String, String> externalConfig, final boolean envelopeEnabled) {
        return writerConstructor.create(outputStream, fields,
                Objects.isNull(externalConfig) ? Collections.emptyMap() : externalConfig, envelopeEnabled);
    }

    /**
     * A functional interface to crate the OutputWriter with.
     */
    @FunctionalInterface
    interface WriterConstructor {
        /**
         * Constructs an OutputWriter.
         *
         * @param outputStream
         *            the output stream to write to.
         * @param fields
         *            the fields to write to the stream. May not be empty.
         * @param externalConfig
         *            an external configuration. May be empty.
         * @param envelopeEnabled
         *            {@code true} if the envelope is enabled.
         * @return an OutputWriter implementation.
         */
        OutputWriter create(OutputStream outputStream, Collection<OutputField> fields,
                Map<String, String> externalConfig, boolean envelopeEnabled);
    }
}
