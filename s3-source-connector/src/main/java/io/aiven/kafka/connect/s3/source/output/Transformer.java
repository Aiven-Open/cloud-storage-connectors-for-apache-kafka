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

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;

/**
 * Defines a transform from the S3 input stream to the data @{code byte[]} found in a
 * {@code ConsumerRecord<byte[], byte[]>}.
 */
public interface Transformer {

    /**
     * The name this Transformer is known as.
     *
     * @return the Transformer name.
     */
    String getName();

    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);

    /**
     * Converts the S3 base InputStream into one or more {@code byte[]}.
     *
     * @param inputStream
     *            the InputStream from an {@link S3Object}
     * @param topic
     *            The Kafka topic the data is destined for.
     * @param s3SourceConfig
     *            the source configuration.
     * @return an {@code Iterator} over the {@code byte[]} constructed from the input.
     * @throws BadDataException
     *             If the data conversion fails.
     */
    Iterator<byte[]> byteArrayIterator(InputStream inputStream, String topic, S3SourceConfig s3SourceConfig)
            throws BadDataException;
}
