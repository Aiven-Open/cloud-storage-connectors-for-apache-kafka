/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.s3.config;

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
final class S3SinkCredentialsConfigTest {
    @Test
    void emptyAwsAccessKeyID() {
        final Map<String, String> props = S3SinkConfigTest.defaultProperties();
        props.put(AWS_ACCESS_KEY_ID, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [hidden] for configuration aws.access.key.id: Password must be non-empty");

        props.remove(AWS_ACCESS_KEY_ID);
        props.put(AWS_ACCESS_KEY_ID_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [hidden] for configuration aws.access.key.id: Password must be non-empty");
    }

    @Test
    void emptyAwsSecretAccessKey() {
        final Map<String, String> props = S3SinkConfigTest.defaultProperties();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value [hidden] for configuration aws.secret.access.key: Password must be non-empty");

        props.remove(AWS_ACCESS_KEY_ID);
        props.remove(AWS_SECRET_ACCESS_KEY);
        props.put(AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value [hidden] for configuration aws.secret.access.key: Password must be non-empty");
    }

    /**
     * Even when no sts role or session name is provided we should be able to create a configuration since it will fall
     * back to using default credentials.
     */
    @Test
    void defaultCredentials() {
        final Map<String, String> props = S3SinkConfigTest.defaultProperties();
        final S3SinkConfig config = new S3SinkConfig(props);
        assertThat(config.getAwsCredentials()).isNull();
        assertThat(config.getCustomCredentialsProvider()).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }
}
