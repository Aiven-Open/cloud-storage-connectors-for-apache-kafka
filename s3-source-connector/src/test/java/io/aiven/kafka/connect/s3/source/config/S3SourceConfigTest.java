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

package io.aiven.kafka.connect.s3.source.config;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import io.aiven.kafka.connect.s3.source.output.TransformerFactory;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.junit.jupiter.api.Test;

final class S3SourceConfigTest {
    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        // aws props
        props.put(S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, "the-bucket");
        props.put(S3SourceConfig.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3SourceConfig.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3SourceConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        // record, topic specific props
        props.put(S3SourceConfig.OUTPUT_FORMAT_KEY, TransformerFactory.DEFAULT_TRANSFORMER_NAME);
        props.put(TARGET_TOPIC_PARTITIONS, "0,1");
        props.put(TARGET_TOPICS, "testtopic");
        props.put(SCHEMA_REGISTRY_URL, "localhost:8081");

        final var conf = new S3SourceConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAccessKeyId().value()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getSecretAccessKey().value()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("the-bucket");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(RegionUtils.getRegion("us-east-1"));

        assertThat(conf.getTargetTopics()).isEqualTo("testtopic");
        assertThat(conf.getTargetTopicPartitions()).isEqualTo("0,1");
        assertThat(conf.getSchemaRegistryUrl()).isEqualTo("localhost:8081");

        assertThat(conf.getS3RetryBackoffDelayMs()).isEqualTo(S3SourceConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxDelayMs())
                .isEqualTo(S3SourceConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxRetries()).isEqualTo(S3SourceConfig.S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT);
    }
}
