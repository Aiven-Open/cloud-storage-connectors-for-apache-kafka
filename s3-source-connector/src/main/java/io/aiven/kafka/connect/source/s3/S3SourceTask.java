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

package io.aiven.kafka.connect.source.s3;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
public class S3SourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SourceConnector.class);

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        Objects.requireNonNull(props, "props hasn't been set");
    }

    @Override
    public List<SourceRecord> poll() {
        return Collections.emptyList();
    }

    @Override
    public void stop() {
    }
}
