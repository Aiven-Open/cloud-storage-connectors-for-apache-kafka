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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OUTPUT_FORMAT_KEY;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

/**
 * The factory for Transformers
 */
public final class TransformerFactory {

    /**
     * The name of the default transformer if none is specified.
     */
    public static final String DEFAULT_TRANSFORMER_NAME = "bytes";

    /**
     * The registered transformers indexed by transformer name.
     */
    private static Map<String, Transformer> registeredTransformers = new HashMap<>();

    static {
        register(new AvroTransformer());
        register(new ByteArrayTransformer());
        register(new JsonTransformer());
        register(new ParquetTransformer());
    }

    /**
     * Registers a new transformer. Transformers are registered by the loser case version of their name. If any
     * previously registered transformer has the same name it is replaced.
     *
     * @param transformer
     *            the Transformer to register.
     */
    public static void register(Transformer transformer) {
        registeredTransformers.put(transformer.getName().toLowerCase(Locale.ROOT).trim(), transformer);
    }

    private TransformerFactory() {
        // hidden
    }

    public static Transformer transformer(final S3SourceConfig s3SourceConfig) {
        return transformer(s3SourceConfig.getString(OUTPUT_FORMAT_KEY));
    }

    public static Transformer transformer(final String transformerName) {
        Transformer result = registeredTransformers.get(transformerName.toLowerCase(Locale.ROOT).trim());
        if (result == null) {
            throw new IllegalArgumentException("Unknown output format " + transformerName);
        }
        return result;
    }
}
