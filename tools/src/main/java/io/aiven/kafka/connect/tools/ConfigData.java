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

package io.aiven.kafka.connect.tools;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;

public class ConfigData {

    private final ConfigDef.ConfigKey key;

    public ConfigData(final ConfigDef.ConfigKey key) {
        this.key = key;
    }

    public final String getName() {
        return key.name;
    }
    public final ConfigDef.Type getType() {
        return key.type;
    }
    public final String getDocumentation() {
        return key.documentation;
    }
    public final Object getDefaultValue() {
        return key.defaultValue;
    }
    public final ConfigDef.Validator getValidator() {
        return key.validator;
    }
    public final ConfigDef.Importance getImportance() {
        return key.importance;
    }
    public final String getGroup() {
        return key.group;
    }
    public final int getOrderInGroup() {
        return key.orderInGroup;
    }
    public final ConfigDef.Width getWidth() {
        return key.width;
    }
    public final String getDisplayName() {
        return key.displayName;
    }
    public final List<String> getDependents() {
        return key.dependents;
    }
    public final ConfigDef.Recommender getRecommender() {
        return key.recommender;
    }
    public final boolean isInternalConfig() {
        return key.internalConfig;
    }

}
