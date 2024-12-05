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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMA_REGISTRY_URL;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, sourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public Stream<Object> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        return readAvroRecordsAsStream(inputStreamIOSupplier, datumReader);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final AbstractConfig sourceConfig) {
        return TransformationUtils.serializeAvroRecordToBytes(Collections.singletonList((GenericRecord) record), topic,
                sourceConfig);
    }

    private Stream<Object> readAvroRecordsAsStream(final IOSupplier<InputStream> inputStreamIOSupplier,
            final DatumReader<GenericRecord> datumReader) {
        InputStream inputStream; // NOPMD CloseResource: being closed in try resources iterator
        DataFileStream<GenericRecord> dataFileStream; // NOPMD CloseResource: being closed in try resources iterator
        try {
            // Open input stream from S3
            inputStream = inputStreamIOSupplier.get();

            // Ensure the DataFileStream is initialized correctly with the open stream
            dataFileStream = new DataFileStream<>(inputStream, datumReader);

            // Wrap DataFileStream in a Stream using a custom Spliterator for lazy processing
            return StreamSupport.stream(new AvroRecordSpliterator<>(dataFileStream), false).onClose(() -> {
                try {
                    dataFileStream.close(); // Ensure the reader is closed after streaming
                } catch (IOException e) {
                    LOGGER.error("Error closing BufferedReader: {}", e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            LOGGER.error("Error in DataFileStream: {}", e.getMessage(), e);
            return Stream.empty(); // Return an empty stream if initialization fails
        }
    }

    private static class AvroRecordSpliterator<T> implements Spliterator<T> {
        private final DataFileStream<GenericRecord> dataFileStream;

        public AvroRecordSpliterator(final DataFileStream<GenericRecord> dataFileStream) {
            this.dataFileStream = dataFileStream;
        }

        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
            try {
                if (dataFileStream.hasNext()) {
                    final GenericRecord record = dataFileStream.next();
                    action.accept((T) record);
                    return true;
                }
            } catch (Exception e) { // NOPMD AvoidCatchingGenericException
                LOGGER.error("Error while reading Avro record: {}", e.getMessage(), e);
                return false;
            }
            return false;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null; // Can't split the data stream as DataFileStream is sequential
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE; // We don't know the size upfront
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.NONNULL;
        }
    }

    List<Object> readAvroRecords(final InputStream content, final DatumReader<GenericRecord> datumReader) {
        final List<Object> records = new ArrayList<>();
        try (SeekableByteArrayInput sin = new SeekableByteArrayInput(IOUtils.toByteArray(content))) {
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                reader.forEach(records::add);
            } catch (IOException e) {
                LOGGER.error("Failed to read records from DataFileReader for S3 object stream. Error: {}",
                        e.getMessage(), e);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to initialize SeekableByteArrayInput for S3 object stream. Error: {}", e.getMessage(),
                    e);
        }
        return records;
    }
}
