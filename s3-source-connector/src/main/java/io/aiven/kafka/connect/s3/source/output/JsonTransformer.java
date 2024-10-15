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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMAS_ENABLE;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.codehaus.stax2.ri.SingletonIterator;

/**
 * Defines a transform from the S3 input stream to the JSON formatted data @{code byte[]} found in a
 * {@code ConsumerRecord<byte[], byte[]>}.
 */
public class JsonTransformer implements Transformer {

    @Override
    public String getName() {
        return "json";
    }

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMAS_ENABLE, "false");
    }

    @Override
    public Iterator<byte[]> byteArrayIterator(InputStream inputStream, String topic, S3SourceConfig s3SourceConfig)
            throws BadDataException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return new SingletonIterator<>(objectMapper.writeValueAsBytes(objectMapper.readTree(inputStream)));
        } catch (IOException e) {
            throw new BadDataException(e);
        }
    }
}
