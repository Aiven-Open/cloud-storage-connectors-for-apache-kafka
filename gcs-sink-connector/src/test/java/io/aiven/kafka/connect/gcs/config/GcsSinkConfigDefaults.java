/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.gcs.config;

import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.gcs.GcsSinkConfigDef;
import io.aiven.kafka.connect.gcs.GcsSinkConnector;

public final class GcsSinkConfigDefaults {
    private GcsSinkConfigDefaults() {
    }

    public static Map<String, String> defaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        CommonConfigFragment.setter(properties).connector(GcsSinkConnector.class).name("test-connector");
        properties.put(GcsSinkConfigDef.GCS_BUCKET_NAME_CONFIG, "some-bucket");
        return properties;
    }

    public static Map<String, String> defaultProperties(final Map<String, String> overrides) {
        final Map<String, String> result = defaultProperties();
        result.putAll(overrides);
        return result;
    }
}
