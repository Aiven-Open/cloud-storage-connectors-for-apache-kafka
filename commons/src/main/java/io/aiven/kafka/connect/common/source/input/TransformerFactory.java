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

import static io.aiven.kafka.connect.common.config.TransformerFragment.SCHEMAS_ENABLE;

import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;

import io.confluent.connect.avro.AvroData;

/**
 * A factory to create Transformers.
 */
public final class TransformerFactory {
    /** The cache size for systems that read Avro data */
    public static final int CACHE_SIZE = 100;

    private TransformerFactory() {
        // hidden
    }

    /**
     * Gets a configured Transformer.
     *
     * @param inputFormat
     *            The input format for the transformer.
     * @return the Transformer for the specified input format.
     */
    public static Transformer getTransformer(final InputFormat inputFormat) {
        switch (inputFormat) {
            case AVRO :
                return new AvroTransformer(new AvroData(CACHE_SIZE));
            case PARQUET :
                return new ParquetTransformer(new AvroData(CACHE_SIZE));
            case JSONL :
                final JsonConverter jsonConverter = new JsonConverter();
                jsonConverter.configure(Map.of(SCHEMAS_ENABLE, "false"), false);
                return new JsonTransformer(jsonConverter);
            case BYTES :
                return new ByteArrayTransformer();
            default :
                throw new IllegalArgumentException("Unknown input format in configuration: " + inputFormat);
        }
    }
}
