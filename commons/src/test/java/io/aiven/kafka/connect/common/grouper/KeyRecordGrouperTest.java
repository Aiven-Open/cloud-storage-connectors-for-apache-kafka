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

package io.aiven.kafka.connect.common.grouper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Lists.list;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;

final class KeyRecordGrouperTest {

    private static final SinkRecord T0P0R0 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null,
            0);
    private static final SinkRecord T0P0R1 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null,
            1);
    private static final SinkRecord T0P0R2 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null,
            null, 2);
    private static final SinkRecord T0P0R3 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null,
            null, 3);
    private static final SinkRecord T0P0R4 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null,
            4);
    private static final SinkRecord T0P0R5 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null,
            5);

    private static final SinkRecord T0P1R0 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null,
            10);
    private static final SinkRecord T0P1R1 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null,
            11);
    private static final SinkRecord T0P1R2 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null,
            null, 12);
    private static final SinkRecord T0P1R3 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null,
            13);

    private static final SinkRecord T1P1R0 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null,
            1000);
    private static final SinkRecord T1P1R1 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null,
            1001);
    private static final SinkRecord T1P1R2 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null,
            null, 1002);
    private static final SinkRecord T1P1R3 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null,
            1003);

    @Test
    void empty() {
        final Template filenameTemplate = Template.of("{{key}}");
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);
        assertThat(grouper.records()).isEmpty();
    }

    @Test
    void eachKeyInSinglePartition() {
        final Template filenameTemplate = Template.of("{{key}}");
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);

        grouper.put(T0P0R0);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);
        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records).containsOnly(entry("a", list(T0P0R4)), entry("b", list(T0P0R5)),
                entry("null", list(T0P0R3)));
    }

    @Test
    void keysInMultiplePartitions() {
        final Template filenameTemplate = Template.of("{{key}}");
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);

        grouper.put(T0P0R0);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);
        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);
        grouper.put(T0P1R3);

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records).containsOnly(entry("a", list(T1P1R3)), entry("b", list(T0P1R1)), entry("c", list(T0P1R3)),
                entry("d", list(T1P1R1)), entry("null", list(T1P1R2)));
    }
}
