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

package io.aiven.kafka.connect.common.config.extractors;

import java.util.stream.Stream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HeaderValueExtractorTest {

    static SinkRecord record1 = new SinkRecord("topic", 0, null, null, null, null, 0, 0L, TimestampType.CREATE_TIME,
            new ConnectHeaders().add("h1", "value1", Schema.STRING_SCHEMA)
                    .add("h2", "v2", Schema.STRING_SCHEMA)
                    .add("b1", true, Schema.BOOLEAN_SCHEMA)
                    .add("b2", false, Schema.BOOLEAN_SCHEMA)
                    .add("i1", null, Schema.OPTIONAL_INT32_SCHEMA)
                    .add("i2", 17, Schema.OPTIONAL_INT32_SCHEMA)
                    .add("i3", 99, Schema.INT32_SCHEMA)
                    .add("i1", null, Schema.OPTIONAL_INT64_SCHEMA)
                    .add("l2", 17L, Schema.OPTIONAL_INT64_SCHEMA)
                    .add("l3", 99L, Schema.INT64_SCHEMA)
                    .add("dup", "one", Schema.STRING_SCHEMA)
                    .add("dup", "two", Schema.STRING_SCHEMA));

    public static Stream<Arguments> testData() {
        return Stream.of(Arguments.of(record1, "h1", "value1"), Arguments.of(record1, "h2", "v2"),
                Arguments.of(record1, "b1", true), Arguments.of(record1, "b2", false),
                Arguments.of(record1, "i1", null), Arguments.of(record1, "i2", 17), Arguments.of(record1, "i3", 99),
                Arguments.of(record1, "i1", null), Arguments.of(record1, "l2", 17L), Arguments.of(record1, "l3", 99L),
                Arguments.of(record1, "dup", "two"), Arguments.of(record1, "xxxxx", null));

    }

    @ParameterizedTest
    @MethodSource("testData")
    void test(SinkRecord record, String headerKey, Object expected) {
        var headerValueExtractor = new HeaderValueExtractor(headerKey);
        assertEquals(expected, headerValueExtractor.extractDataFrom(record));
    }

}
