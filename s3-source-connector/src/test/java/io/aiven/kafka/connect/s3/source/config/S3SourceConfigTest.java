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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

final class S3SourceConfigTest {
    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        S3ConfigFragment.setter(props)
                .accessKeyId("AWS_ACCESS_KEY_ID")
                .accessKeySecret("AWS_SECRET_ACCESS_KEY")
                .bucketName("the-bucket")
                .endpoint("AWS_S3_ENDPOINT")
                .region(Region.US_EAST_1);
        CommonConfigFragment.setter(props).name("S3SourceConfigTest").connector(Connector.class);
        TransformerFragment.setter(props).inputFormat(InputFormat.AVRO).schemaRegistry("localhost:8081");
        SourceConfigFragment.setter(props).targetTopic("testtopic");
        FileNameFragment.setter(props).template("any-old-file");

        final var conf = new S3SourceConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.accessKeyId()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.secretAccessKey()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("the-bucket");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(Region.of("us-east-1"));

        assertThat(conf.getInputFormat()).isEqualTo(InputFormat.AVRO);
        assertThat(conf.getTargetTopic()).isEqualTo("testtopic");
        assertThat(conf.getSchemaRegistryUrl()).isEqualTo("localhost:8081");

        assertThat(conf.getS3RetryBackoffDelayMs()).isEqualTo(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxDelayMs())
                .isEqualTo(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxRetries()).isEqualTo(S3ConfigFragment.S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT);
    }
}
