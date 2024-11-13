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

package io.aiven.kafka.connect.common.config;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
public abstract class CommonConfig extends AbstractConfig {
    protected static final String GROUP_COMPRESSION = "File Compression";
    protected static final String GROUP_FORMAT = "Format";


    /**
     * @deprecated No longer needed.
     */
    @Deprecated
    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) {
        // not required since it is loaded in
    }

    public CommonConfig(ConfigDef definition, Map<?, ?> originals) { // NOPMD
        super(BackoffPolicyConfig.update(definition), originals);
    }

    public Long getKafkaRetryBackoffMs() {
        return new BackoffPolicyConfig(this).getKafkaRetryBackoffMs();
    }

}
