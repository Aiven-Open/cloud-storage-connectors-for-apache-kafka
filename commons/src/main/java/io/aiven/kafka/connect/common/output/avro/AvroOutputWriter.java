/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.connect.common.output.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.OutputStreamWriter;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.output.SinkRecordConverter;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of OutputWriter that writes to Avro files.
 */
public final class AvroOutputWriter extends OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroOutputWriter.class);

    public AvroOutputWriter(final Collection<OutputField> fields, final OutputStream out,
            final Map<String, String> externalConfig, final boolean envelopeEnabled) {
        super(out, new AvroOutputStreamWriter(fields, externalConfig, envelopeEnabled), externalConfig);
    }

    /**
     * An instance of OutputStreamWriter that handles writing the Avro format
     */
    private static final class AvroOutputStreamWriter implements OutputStreamWriter {
        /**
         * The sink record converter for Avro.
         */
        private final SinkRecordConverter sinkRecordConverter;
        /**
         * The Avro schema builder.
         */
        private final AvroSchemaBuilder avroSchemaBuilder;
        /**
         * The Avro configuration.
         */
        private final AvroConfig avroConfiguration;

        /**
         * Lazily constructed Avro schema used in the output stream.
         */
        private Schema avroSchema;
        /**
         * Lazily constructed Avro DataFileWriter.
         */
        private DataFileWriter<GenericRecord> dataFileWriter;

        /**
         * Constructor.
         *
         * @param fields
         *            the fields to output.
         * @param externalConfig
         *            the configuration data for the Avro configuration.
         * @param envelopeEnabled
         *            {@code true if the envelope is enabled}
         */
        AvroOutputStreamWriter(final Collection<OutputField> fields, final Map<String, String> externalConfig,
                final boolean envelopeEnabled) {
            final AvroData avroData = new AvroData(new AvroDataConfig(externalConfig));
            this.sinkRecordConverter = new SinkRecordConverter(fields, avroData, envelopeEnabled);
            this.avroSchemaBuilder = new AvroSchemaBuilder(fields, avroData, envelopeEnabled);
            this.avroConfiguration = AvroConfig.createAvroConfiguration(externalConfig);
        }

        /**
         * Create the data file writer if it does not exist. Requires that {@link #getAvroSchema(SinkRecord)} be called
         * at least once prior.
         *
         * @return the DataFileWriter.
         * @throws IOException
         *             if the writer can not be created.
         */
        private DataFileWriter<GenericRecord> getDataFileWriter(final OutputStream outputStream) throws IOException {
            if (dataFileWriter == null) {
                final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
                dataFileWriter = new DataFileWriter<>(writer);
                dataFileWriter.setCodec(avroConfiguration.codecFactory());
                // create with output stream that does not close the underlying stream.
                dataFileWriter.create(avroSchema, CloseShieldOutputStream.wrap(outputStream));
            }
            return dataFileWriter;
        }

        /**
         * Creates the Avro schema if necessary. Will throw an exception if the record schema does not match the output
         * Avro schema.
         *
         * @param sinkRecord
         *            the record to be written.
         * @return the file Avro schema.
         * @throws IOException
         *             if the record schema does not match the file schema.
         */
        private Schema getAvroSchema(final SinkRecord sinkRecord) throws IOException {
            if (avroSchema == null) {
                avroSchema = avroSchemaBuilder.buildSchema(sinkRecord);
                LOGGER.debug("Record schema is: {}", avroSchema);
            } else {
                final Schema otherSchema = avroSchemaBuilder.buildSchema(sinkRecord);
                if (!avroSchema.equals(otherSchema)) {
                    LOGGER.error("Illegal Schema Change. {}", otherSchema);
                    throw new IOException("Illegal schema change");
                }
            }
            return avroSchema;
        }

        @Override
        public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
            final GenericRecord datum = sinkRecordConverter.convert(record, getAvroSchema(record));
            getDataFileWriter(outputStream).append(datum);
        }

        @Override
        public void stopWriting(final OutputStream outputStream) throws IOException {
            if (dataFileWriter != null) {
                dataFileWriter.close();
            }
        }
    }
}
