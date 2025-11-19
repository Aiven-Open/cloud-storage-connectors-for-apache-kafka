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

package io.aiven.kafka.connect.common.config.validators;

import java.util.Objects;
import java.util.function.BiConsumer;

import org.apache.kafka.common.config.ConfigDef;

public class UsageLoggingValidator implements ConfigDef.Validator {
    private final ConfigDef.Validator delegate;
    private final BiConsumer<String, Object> nameValueLogger;

    public UsageLoggingValidator(final ConfigDef.Validator delegate, final BiConsumer<String, Object> nameValueLogger) {
        this.delegate = Objects.requireNonNull(delegate);
        this.nameValueLogger = Objects.requireNonNull(nameValueLogger);
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (Objects.nonNull(value)) {
            nameValueLogger.accept(name, value);
        }
        delegate.ensureValid(name, value);
    }
}
