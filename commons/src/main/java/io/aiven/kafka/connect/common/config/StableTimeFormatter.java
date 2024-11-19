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

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

public class StableTimeFormatter {
    private static final Map<String, DateTimeFormatter> TIMESTAMP_FORMATTERS = Map.of("yyyy",
            DateTimeFormatter.ofPattern("yyyy"), "MM", DateTimeFormatter.ofPattern("MM"), "dd",
            DateTimeFormatter.ofPattern("dd"), "HH", DateTimeFormatter.ofPattern("HH"));

    private final Function<SinkRecord, Function<VariableTemplatePart.Parameter, String>> formatter;

    public StableTimeFormatter(final TimestampSource timestampSource) {
        this.formatter = record -> {
            final var time = timestampSource.time(record);
            return parameter -> time.format(TIMESTAMP_FORMATTERS.get(parameter.getValue()));
        };
    }
    public Function<VariableTemplatePart.Parameter, String> apply(final SinkRecord record) {
        return formatter.apply(record);
    }
}
