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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;

class StableTimeFormatterTest {
    private final TimestampSource timestampSource = new TimestampSource() {
        int counter = 10;
        @Override
        public ZonedDateTime time(final SinkRecord record) {
            return ZonedDateTime.of(2021, 2, 3, counter++, 4, 5, 0, ZoneOffset.UTC);
        }

        @Override
        public Type type() {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    void testStableTime() {
        final Template template = Template.of(
                "yy-{{timestamp:unit=yyyy}},MM-{{timestamp:unit=MM}},dd-{{timestamp:unit=dd}},HH1-{{timestamp:unit=HH}},HH2-{{timestamp:unit=HH}}");

        final var stableTimeFormatter = new StableTimeFormatter(timestampSource);
        final var currentRecord = new SinkRecord("topic", 0, null, null, null, null, 0);
        final var result = template.instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, stableTimeFormatter.apply(currentRecord))
                .render();
        // used to be yy-2021,MM-02,dd-03,HH1-13,HH2-14, if we dont cache the time
        assertEquals("yy-2021,MM-02,dd-03,HH1-10,HH2-10", result);

        final var result2 = template.instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, stableTimeFormatter.apply(currentRecord))
                .render();
        // ensure time isnt cached across records
        assertEquals("yy-2021,MM-02,dd-03,HH1-11,HH2-11", result2);
    }

}
