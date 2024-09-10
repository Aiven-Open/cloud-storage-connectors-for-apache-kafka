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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports" })
final public class S3SourceConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    public S3SourceConfig() {
        validate();
    }

    static Map<String, String> preprocessProperties() {
        return Collections.emptyMap();
    }

    public static ConfigDef configDef() {
        return new S3SourceConfigDef();
    }

    private void validate() {
        LOGGER.debug("Validating config.");
    }
}
