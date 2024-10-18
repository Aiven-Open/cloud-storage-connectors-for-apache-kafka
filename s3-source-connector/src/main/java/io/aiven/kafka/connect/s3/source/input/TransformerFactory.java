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

package io.aiven.kafka.connect.s3.source.input;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.INPUT_FORMAT_KEY;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public final class TransformerFactory {

    private TransformerFactory() {
        // hidden
    }
    public static Transformer getWriter(final S3SourceConfig s3SourceConfig) {
        final InputFormat inputFormatEnum = s3SourceConfig.getInputFormat();
        switch (inputFormatEnum) {
            case AVRO :
                return new AvroTransformer();
            case PARQUET :
                return new ParquetTransformer();
            case JSONL :
                return new JsonTransformer();
            case BYTES :
                return new ByteArrayTransformer();
            default :
                throw new IllegalArgumentException(
                        "Unknown output format " + s3SourceConfig.getString(INPUT_FORMAT_KEY));
        }
    }
}
