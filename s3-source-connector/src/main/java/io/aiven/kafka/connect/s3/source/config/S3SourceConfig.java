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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SchemaRegistryFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.config.s3.S3SourceBaseConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class S3SourceConfig extends S3SourceBaseConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    public S3SourceConfig(final Map<String, String> properties) {
        super(configDef(), properties);
        validate(); // NOPMD ConstructorCallsOverridableMethod getStsRole is called
    }

    public static ConfigDef configDef() {

        final var configDef = new S3SourceConfigDef();
        S3ConfigFragment.update(configDef);
        SourceConfigFragment.update(configDef);
        FileNameFragment.update(configDef);
        SchemaRegistryFragment.update(configDef);

        return configDef;
    }

    private void validate() {
        LOGGER.debug("Validating config.");
    }

}
