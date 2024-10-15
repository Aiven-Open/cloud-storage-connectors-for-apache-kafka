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

package io.aiven.kafka.connect.s3.source.output;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.util.IOUtils;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroWriter implements OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public List<Object> getRecords(final InputStream inputStream, final String topic, final int topicPartition,
            final S3SourceConfig s3SourceConfig) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        return readAvroRecords(inputStream, datumReader);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final S3SourceConfig s3SourceConfig) {
        return OutputUtils.serializeAvroRecordToBytes(Collections.singletonList((GenericRecord) record), topic,
                s3SourceConfig);
    }

    List<Object> readAvroRecords(final InputStream content, final DatumReader<GenericRecord> datumReader) {
        final List<Object> records = new ArrayList<>();
        try (SeekableByteArrayInput sin = new SeekableByteArrayInput(IOUtils.toByteArray(content))) {
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                reader.forEach(records::add);
            } catch (IOException e) {
                LOGGER.error("Error in reading s3 object stream " + e.getMessage());
            }
        } catch (IOException e) {
            LOGGER.error("Error in reading s3 object stream " + e.getMessage());
        }
        return records;
    }
}
