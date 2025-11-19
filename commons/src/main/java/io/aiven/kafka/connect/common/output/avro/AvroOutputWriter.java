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
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AvroOutputWriter extends OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroOutputWriter.class);

    private final AvroSchemaBuilder avroSchemaBuilder;
    private final SinkRecordConverter sinkRecordConverter;

    public AvroOutputWriter(final Collection<OutputField> fields, final OutputStream out,
            final Map<String, String> externalConfig, final boolean envelopeEnabled) {
        super(out, new OutputStreamWriterStub(), externalConfig);
        final AvroData avroData = new AvroData(new AvroDataConfig(externalConfig));
        this.sinkRecordConverter = new SinkRecordConverter(fields, avroData, envelopeEnabled);
        this.avroSchemaBuilder = new AvroSchemaBuilder(fields, avroData, envelopeEnabled);
    }

    @Override
    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        final AvroConfig avroConfiguration = AvroConfig.createAvroConfiguration(externalConfiguration);
        final Schema avroSchema = avroSchemaBuilder.buildSchema(sinkRecords.iterator().next());
        LOGGER.debug("Record schema is: {}", avroSchema);

        final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)) {
            dataFileWriter.setCodec(avroConfiguration.codecFactory());
            dataFileWriter.create(avroSchema, outputStream);
            for (final SinkRecord record : sinkRecords) {
                final GenericRecord datum = sinkRecordConverter.convert(record, avroSchema);
                dataFileWriter.append(datum);
            }
        }
    }

    @Override
    public void writeRecord(final SinkRecord record) throws IOException {
        writeRecords(List.of(record));
    }

    private static final class OutputStreamWriterStub implements OutputStreamWriter {
        @Override
        public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        }
    }
}
