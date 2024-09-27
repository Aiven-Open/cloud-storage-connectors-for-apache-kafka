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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
public final class TestConfig extends AivenCommonConfig {
    private TestConfig(final Map<String, String> originals) {
        super(configDef(), originals);
        validate();
    }

    private static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        addFileConfigGroup(configDef, "File", "Test", 1, null);
        addOutputFieldsFormatConfigGroup(configDef, OutputFieldType.VALUE);
        AivenCommonConfig.addCommonConfig(configDef);
        return configDef;
    }
    public final static class Builder {
        private final Map<String, String> props;

        public Builder() {
            props = new HashMap<>();
        }
        public Builder withMinimalProperties() {
            return withProperty(FILE_COMPRESSION_TYPE_CONFIG, "none");
        }
        public Builder withProperty(final String key, final String value) {
            props.put(key, value);
            return this;
        }
        public Builder withoutProperty(final String key) {
            props.remove(key);
            return this;
        }

        public TestConfig build() {
            return new TestConfig(props);
        }

    }

    void validate() {
    }
}
