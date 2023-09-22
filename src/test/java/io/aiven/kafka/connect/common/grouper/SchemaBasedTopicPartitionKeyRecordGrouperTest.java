/*
 * Copyright 2021 Aiven Oy
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

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Lists.list;

final class SchemaBasedTopicPartitionKeyRecordGrouperTest {

    static final SinkRecord KT0P0R0 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 0);
    static final SinkRecord KT0P0R1 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(1).build(), "some_key",
            SchemaBuilder.string().optional().version(1).build(), null, 1);
    static final SinkRecord KT0P0R2 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 2);
    static final SinkRecord KT0P0R3 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 3);
    static final SinkRecord KT0P0R4 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(2).build(), "some_key",
            SchemaBuilder.string().optional().version(1).build(), null, 4);
    static final SinkRecord KT0P0R5 = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(2).build(), "some_key",
            SchemaBuilder.string().optional().version(1).build(), null, 5);

    static final SinkRecord RT0P1R0 = new SinkRecord(
            "topic0", 1,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 10);
    static final SinkRecord RT0P1R1 = new SinkRecord(
            "topic0", 1,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 11);
    static final SinkRecord RT0P1R2 = new SinkRecord(
            "topic0", 1,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(2).build(), null, 12);
    static final SinkRecord RT0P1R3 = new SinkRecord(
            "topic0", 1,
            SchemaBuilder.string().optional().version(1).build(), "some_key",
            SchemaBuilder.string().optional().version(2).build(), null, 13);

    static final SinkRecord KRT1P1R0 = new SinkRecord(
            "topic1", 0,
            SchemaBuilder.string().optional().version(1).build(), "some_key",
            SchemaBuilder.string().optional().version(1).build(), null, 1000);
    static final SinkRecord KRT1P1R1 = new SinkRecord(
            "topic1", 0,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 1001);
    static final SinkRecord KRT1P1R2 = new SinkRecord(
            "topic1", 0,
            SchemaBuilder.string().optional().version(1).build(), null,
            SchemaBuilder.string().optional().version(1).build(), null, 1002);
    static final SinkRecord KRT1P1R3 = new SinkRecord(
            "topic1", 0,
            SchemaBuilder.string().optional().version(2).build(), "some_key",
            SchemaBuilder.string().optional().version(2).build(), null, 1003);

    static final TimestampSource DEFAULT_TS_SOURCE =
            TimestampSource.of(TimestampSource.Type.WALLCLOCK);

    @Test
    void rotateOnKeySchemaChanged() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{key}}-{{start_offset}}");
        final RecordGrouper grouper =
                new SchemaBasedTopicPartitionKeyRecordGrouper(
                        filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(KT0P0R0);
        grouper.put(KT0P0R1);
        grouper.put(KT0P0R2);
        grouper.put(KT0P0R3);
        grouper.put(KT0P0R4);
        grouper.put(KT0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-0-null-0", list(KT0P0R0, KT0P0R2, KT0P0R3)),
                entry("topic0-0-some_key-1", list(KT0P0R1)),
                entry("topic0-0-some_key-4", list(KT0P0R4, KT0P0R5))
            );
    }

    @Test
    void rotateOnValueSchemaChanged() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{key}}-{{start_offset}}");
        final RecordGrouper grouper =
                new SchemaBasedTopicPartitionKeyRecordGrouper(
                        filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(RT0P1R0);
        grouper.put(RT0P1R1);
        grouper.put(RT0P1R2);
        grouper.put(RT0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-1-null-10", list(RT0P1R0, RT0P1R1)),
                entry("topic0-1-null-12", list(RT0P1R2)),
                entry("topic0-1-some_key-13", list(RT0P1R3))
            );
    }

    @Test
    void rotateOnValueSchemaChangedAndButchSize() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{key}}-{{start_offset}}");
        final RecordGrouper grouper =
                new SchemaBasedTopicPartitionKeyRecordGrouper(
                        filenameTemplate, 2, DEFAULT_TS_SOURCE);

        grouper.put(KRT1P1R0);
        grouper.put(KRT1P1R1);
        grouper.put(KRT1P1R2);
        grouper.put(KRT1P1R3);

        final var records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic1-0-some_key-1000", list(KRT1P1R0)),
                entry("topic1-0-null-1001", list(KRT1P1R1, KRT1P1R2)),
                entry("topic1-0-some_key-1003", list(KRT1P1R3))
            );
    }
}
