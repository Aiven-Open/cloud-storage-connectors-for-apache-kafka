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

package io.aiven.kafka.connect.common.output.parquet;

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
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ParquetOutputWriter extends OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetOutputWriter.class);

    private final SinkRecordConverter sinkRecordConverter;

    private final ParquetSchemaBuilder parquetSchemaBuilder;

    private Schema parquetSchema;

    private ParquetWriter<GenericRecord> parquetWriter;

    public ParquetOutputWriter(final Collection<OutputField> fields, final OutputStream out,
            final Map<String, String> externalConfig, final boolean envelopeEnabled) {
        super(new ParquetPositionOutputStream(out), new OutputStreamWriterStub(), externalConfig);
        final var avroData = new AvroData(new AvroDataConfig(externalConfig));
        this.sinkRecordConverter = new SinkRecordConverter(fields, avroData, envelopeEnabled);
        this.parquetSchemaBuilder = new ParquetSchemaBuilder(fields, avroData, envelopeEnabled);
    }

    @Override
    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        if (parquetWriter == null) {
            // One Parquet file per output stream: the writer stays open across calls and writes the footer on close.
            // Parquet and Avro record groupers start a new file on a schema change, so one schema fits the file.
            final var parquetConfig = new ParquetConfig(externalConfiguration);
            parquetSchema = parquetSchemaBuilder.buildSchema(sinkRecords.iterator().next());
            LOGGER.debug("Record schema is: {}", parquetSchema);
            parquetWriter = AvroParquetWriter.<GenericRecord>builder(new ParquetOutputFile())
                    .withSchema(parquetSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withConf(parquetConfig.parquetConfiguration())
                    .withCompressionCodec(parquetConfig.compressionCodecName())
                    .build();
        }
        for (final var record : sinkRecords) {
            parquetWriter.write(sinkRecordConverter.convert(record, parquetSchema));
        }
    }

    @Override
    public void writeRecord(final SinkRecord record) throws IOException {
        this.writeRecords(List.of(record));
    }

    @Override
    public void close() throws IOException {
        try {
            if (parquetWriter != null) {
                parquetWriter.close();
            }
        } finally {
            super.close();
        }
    }

    private static final class OutputStreamWriterStub implements OutputStreamWriter {
        @Override
        public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        }
    }

    private class ParquetOutputFile implements OutputFile {

        @Override
        public PositionOutputStream create(final long blockSizeHint) throws IOException {
            return (ParquetPositionOutputStream) outputStream;
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

}
