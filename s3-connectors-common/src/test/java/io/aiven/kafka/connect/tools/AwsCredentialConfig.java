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

package io.aiven.kafka.connect.tools;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.s3.S3BaseConfig;

public class AwsCredentialConfig extends S3BaseConfig {
    public AwsCredentialConfig(final Map<String, String> properties) {
        super(configDef(new ConfigDef()), handleDeprecatedYyyyUppercase(properties));
    }

    public static ConfigDef configDef(ConfigDef configDef) { // NOPMD UnusedAssignment
        addS3RetryPolicies(configDef);
        addAwsConfigGroup(configDef);
        addAwsStsConfigGroup(configDef);
        addDeprecatedConfiguration(configDef);
        addS3SinkConfig(configDef);
        return configDef;
    }
}
