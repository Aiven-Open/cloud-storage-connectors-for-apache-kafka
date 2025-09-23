/*
 * Copyright 2025 Aiven Oy
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

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.ParquetTestingFixture;
import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;

/**
 * A testing feature to generate Parquet data.
 */
public class ParquetTestDataFixture {
    /**
     * Generate the specified number of parquet records in a byte array.
     *
     * @param name
     *            the name to be used in each object. Each object will have a "name" property with the value of
     *            {@code name} followed by the record number.
     * @param numOfRecords
     *            The number of records to generate.
     * @return A byte array containing the specified number of parquet records.
     * @throws IOException
     *             if the data can not be written.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] generateMockParquetData(final String name, final int numOfRecords) throws IOException {
        Schema schema = ParquetTestingFixture.PARQUET_SCHEMA;

        final List<Struct> allParquetRecords = new ArrayList<>();
        // Write records to the Parquet file
        for (int i = 0; i < numOfRecords; i++) {
            allParquetRecords.add(new Struct(schema).put("name", name + i).put("age", 30).put("email", name + "@test"));
        }

        // Create a Parquet writer
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (var parquetWriter = new ParquetOutputWriter(
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)), outputStream,
                Collections.emptyMap(), false)) {
            int counter = 0;
            final var sinkRecords = new ArrayList<SinkRecord>();
            for (final var r : allParquetRecords) {
                final var sinkRecord = new SinkRecord( // NOPMD AvoidInstantiatingObjectsInLoops
                        "some-topic", 1, STRING_SCHEMA, "some-key-" + counter, schema, r, 100L, 1000L + counter,
                        TimestampType.CREATE_TIME, null);
                sinkRecords.add(sinkRecord);
                counter++;
            }
            parquetWriter.writeRecords(sinkRecords);
        }
        return outputStream.toByteArray();
    }
}
