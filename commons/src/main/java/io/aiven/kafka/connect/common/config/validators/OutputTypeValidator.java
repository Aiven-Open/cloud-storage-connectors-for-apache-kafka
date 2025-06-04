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

package io.aiven.kafka.connect.common.config.validators;

import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.FormatType;

import org.apache.commons.lang3.StringUtils;

public class OutputTypeValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        final String valueStr = value == null ? null : value.toString();
        if (StringUtils.isBlank(valueStr)) {
            throw new ConfigException(name, "must not be empty or not set");
        }
        try {
            FormatType.valueOf(valueStr.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new ConfigException(name, valueStr, "Supported values are: " + this);
        }
    }

    @Override
    public String toString() {
        return FormatType.names().stream().map(s -> "'" + s + "'").collect(Collectors.joining(", "));
    }
}
