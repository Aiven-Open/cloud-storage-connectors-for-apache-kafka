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

package io.aiven.kafka.connect.common.config;

import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.parquet.ParquetOutputWriter;

import org.testcontainers.shaded.org.apache.commons.io.function.IOSupplier;

/**
 * Test fixture to generate standard parquet file.
 */
public final class ParquetTestingFixture {

    /**
     * The schema for the test cases
     */
    public final static Schema PARQUET_SCHEMA = ParquetTestDataFixture.PARQUET_SCHEMA;

    private ParquetTestingFixture() {
        // do int instantiate
    }

    /**
     * Writes 100 parquet records to the file specified using the default schema. The topic "some-topic" will be used
     * for each record. "some-key-#" will be used for each key.
     *
     * @param outputFilePath
     *            the path the to the output file.
     * @param name
     *            the name used for each record. The record number will be appended to the name.
     * @throws IOException
     *             on output error.
     */
    public static Path writeParquetFile(final Path outputFilePath, final String name) throws IOException {
        return ParquetTestDataFixture.writeParquetFile(outputFilePath, name);
    }
}
