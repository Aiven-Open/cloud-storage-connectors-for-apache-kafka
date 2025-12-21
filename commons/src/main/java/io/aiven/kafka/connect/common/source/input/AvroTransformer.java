/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.common.source.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTransformer extends Transformer {

    private final AvroData avroData;

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTransformer.class);

    // GZIP file format magic number (RFC 1952)
    private static final byte GZIP_MAGIC_BYTE_1 = (byte) 0x1f;
    private static final byte GZIP_MAGIC_BYTE_2 = (byte) 0x8b;

    AvroTransformer(final AvroData avroData) {
        super();
        this.avroData = avroData;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            private DataFileStream<GenericRecord> dataFileStream;
            private final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

            @Override
            protected void inputOpened(final InputStream input) throws IOException {
                final InputStream bufferedInput = new BufferedInputStream(input);

                // Peek at the first 2 bytes to detect GZIP compression
                bufferedInput.mark(2);
                final byte[] magicBytes = new byte[2];
                final int bytesRead = bufferedInput.read(magicBytes);
                bufferedInput.reset();

                final boolean isGzipCompressed = bytesRead == 2 && magicBytes[0] == GZIP_MAGIC_BYTE_1
                        && magicBytes[1] == GZIP_MAGIC_BYTE_2;

                try {
                    final InputStream sourceStream = isGzipCompressed
                            ? new GZIPInputStream(bufferedInput)
                            : bufferedInput;
                    dataFileStream = new DataFileStream<>(sourceStream, datumReader);
                } catch (IOException e) {
                    LOGGER.error("Error initializing Avro DataFileStream (GZIP compressed: {}): {}", isGzipCompressed,
                            e.getMessage(), e);
                    throw e;
                }
            }

            @Override
            public void doClose() {
                if (dataFileStream != null) {
                    try {
                        dataFileStream.close();
                    } catch (IOException e) {
                        LOGGER.error("Error closing reader: {}", e.getMessage(), e);
                    }
                }
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                if (dataFileStream.hasNext()) {
                    final GenericRecord record = dataFileStream.next();
                    action.accept(avroData.toConnectData(record.getSchema(), record));
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA,
                ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
