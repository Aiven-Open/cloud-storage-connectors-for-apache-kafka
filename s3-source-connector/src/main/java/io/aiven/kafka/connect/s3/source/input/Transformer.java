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

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;

public interface Transformer {

    // TODO make this method accept an S3OffsetManagerEntry and update the values in the configuration directly.
    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);

    // TODO make this method accept an S3OffsetManagerEntry to retrieve the topic an topicParitiion.
    Stream<Object> getRecords(IOSupplier<InputStream> inputStreamIOSupplier, String topic, int topicPartition,
            S3SourceConfig s3SourceConfig);


    byte[] getValueBytes(Object record, String topic, S3SourceConfig s3SourceConfig);
}
