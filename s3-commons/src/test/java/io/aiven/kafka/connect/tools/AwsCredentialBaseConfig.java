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

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.config.s3.S3SinkBaseConfig;

public class AwsCredentialBaseConfig extends S3SinkBaseConfig {
    public AwsCredentialBaseConfig(final Map<String, String> properties) {
        super(configDef(), properties);
    }

    public static SinkCommonConfigDef configDef() { // NOPMD UnusedAssignment
        final SinkCommonConfigDef configDef = new SinkCommonConfigDef(OutputFieldType.VALUE, CompressionType.NONE);
        S3ConfigFragment.update(configDef);
        return configDef;
    }
}
