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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AVRO_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.BYTE_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.JSON_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.PARQUET_OUTPUT_FORMAT;

public final class OutputWriterFactory {

    private OutputWriterFactory() {
        throw new UnsupportedOperationException("Class cannot be instantiated");
    }
    public static OutputWriter getWriter(final String outputFormat, final String bucket) {
        switch (outputFormat) {
            case AVRO_OUTPUT_FORMAT :
                return new AvroWriter(bucket);
            case PARQUET_OUTPUT_FORMAT :
                return new ParquetWriter(bucket);
            case JSON_OUTPUT_FORMAT :
                return new JsonWriter(bucket);
            case BYTE_OUTPUT_FORMAT :
                return new ByteArrayWriter(bucket);
            default :
                throw new IllegalArgumentException("Unknown output format: " + outputFormat);
        }
    }
}
