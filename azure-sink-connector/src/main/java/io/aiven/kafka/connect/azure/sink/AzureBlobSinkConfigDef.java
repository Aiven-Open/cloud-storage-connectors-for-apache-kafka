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

package io.aiven.kafka.connect.azure.sink;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

public final class AzureBlobSinkConfigDef extends SinkCommonConfig.SinkCommonConfigDef {

    private final static Pattern CONTAINER_NAME_PATTERN = Pattern.compile("[0-9a-z][0-9a-z\\-]+[0-9a-z]");

    /**
     * From Azure documentation:
     * <ul>
     * <li>Container names must start or end with a letter or number, and can contain only letters, numbers, and the
     * hyphen/minus (-) character.</li>
     * <li>Every hyphen/minus (-) character must be immediately preceded and followed by a letter or number; consecutive
     * hyphens aren't permitted in container names.</li>
     * <li>All letters in a container name must be lowercase.</li>
     * <li>Container names must be from 3 through 63 characters long.</li>
     * </ul>
     */
    public static final ConfigDef.Validator CONTAINER_NAME_VALIDATOR = ConfigDef.CompositeValidator
            .of(ConfigDef.LambdaValidator.with((name, value) -> {
                final int len = value == null ? 0 : value.toString().length();
                if (len < 3 || len > 63) {
                    throw new ConfigException(name, value, "names must be from 3 through 63 characters long.");
                }
            }, () -> "must be from 3 through 63 characters long"), ConfigDef.LambdaValidator.with((name, value) -> {
                if (value.toString().contains("--")) {
                    throw new ConfigException(name, value,
                            "Every hyphen/minus (-) character must be immediately preceded and followed by a letter or number; consecutive hyphens aren't permitted in container names.");
                }
            }, () -> "consecutive hyphens aren't permitted in container names"),
                    // regex last for speed
                    ConfigDef.LambdaValidator.with((name, value) -> {
                        if (!CONTAINER_NAME_PATTERN.matcher(value.toString()).matches()) {
                            throw new ConfigException(name, value,
                                    "must start or end with a letter or number, and can contain only lower case letters, numbers, and the hyphen/minus (-) character.");
                        }
                    }, () -> "start or end with a letter or number, and can contain only lower case letters, numbers, and the hyphen/minus (-) character"));

    AzureBlobSinkConfigDef() {
        super(OutputFieldType.VALUE, CompressionType.NONE);
        AzureBlobConfigFragment.update(this, true);
    }

    @Override
    public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
        final Map<String, ConfigValue> result = super.multiValidate(valueMap);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(result);
        new AzureBlobConfigFragment(dataAccess).validate(result);
        return result;
    }
}
