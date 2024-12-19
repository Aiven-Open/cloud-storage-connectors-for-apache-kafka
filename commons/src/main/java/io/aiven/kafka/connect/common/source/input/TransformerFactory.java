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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMAS_ENABLE;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.SchemaRegistryFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import io.confluent.connect.avro.AvroData;

public final class TransformerFactory {

    public static final int CACHE_SIZE = 100;

    private TransformerFactory() {
        // hidden
    }
    public static Transformer getTransformer(final SourceCommonConfig sourceConfig) {
        final InputFormat inputFormatEnum = new SchemaRegistryFragment(sourceConfig).getInputFormat();
        switch (inputFormatEnum) {
            case AVRO :
                return new AvroTransformer(new AvroData(CACHE_SIZE));
            case PARQUET :
                return new ParquetTransformer(new AvroData(CACHE_SIZE));
            case JSONL :
                final JsonConverter jsonConverter = new JsonConverter();
                configureJsonConverter(jsonConverter);
                return new JsonTransformer(jsonConverter);
            case BYTES :
                return new ByteArrayTransformer();
            default :
                throw new IllegalArgumentException(
                        "Unknown input format in configuration: " + sourceConfig.getString(INPUT_FORMAT_KEY));
        }
    }

    private static void configureJsonConverter(final JsonConverter jsonConverter) {
        final Map<String, String> config = new HashMap<>();
        config.put(SCHEMAS_ENABLE, "false");
        jsonConverter.configure(config, false);
    }
}
