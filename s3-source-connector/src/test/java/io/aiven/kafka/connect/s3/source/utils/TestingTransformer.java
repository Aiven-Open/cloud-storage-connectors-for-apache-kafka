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

package io.aiven.kafka.connect.s3.source.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.nimbusds.jose.util.IOUtils;
import org.apache.commons.io.function.IOSupplier;

public class TestingTransformer implements Transformer {

    public static String transform(String original) {
        return String.format("Transformed(%s)", original);
    }
    @Override
    public void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig) {
        config.put("TestingTransformer", "True");
    }

    @Override
    public Stream<Object> getRecords(IOSupplier<InputStream> inputStream, String topic, int topicPartition,
            S3SourceConfig s3SourceConfig) {
        try {
            return Stream.of(transform(IOUtils.readInputStreamToString(inputStream.get())));
        } catch (IOException e) {
            throw new RuntimeException("Error reading input stream", e);
        }
    }

    @Override
    public byte[] getValueBytes(Object record, String topic, S3SourceConfig s3SourceConfig) {
        return ((String) record).getBytes(StandardCharsets.UTF_8);
    }
}
