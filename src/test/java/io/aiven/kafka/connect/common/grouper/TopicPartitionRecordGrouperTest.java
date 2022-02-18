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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Lists.list;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TopicPartitionRecordGrouperTest {

    private static final SinkRecord T0P0R0 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 0);
    private static final SinkRecord T0P0R1 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1);
    private static final SinkRecord T0P0R2 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2);
    private static final SinkRecord T0P0R3 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 3);
    private static final SinkRecord T0P0R4 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 4);
    private static final SinkRecord T0P0R5 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 5);

    private static final SinkRecord T0P1R0 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 10);
    private static final SinkRecord T0P1R1 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 11);
    private static final SinkRecord T0P1R2 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 12);
    private static final SinkRecord T0P1R3 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 13);

    private static final SinkRecord T1P1R0 = new SinkRecord(
        "topic1", 1, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1000);
    private static final SinkRecord T1P1R1 = new SinkRecord(
        "topic1", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1001);
    private static final SinkRecord T1P1R2 = new SinkRecord(
        "topic1", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1002);
    private static final SinkRecord T1P1R3 = new SinkRecord(
        "topic1", 1, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1003);

    private static final SinkRecord T2P1R0 = new SinkRecord(
            "topic2", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2000, 1635375104000L,
            TimestampType.CREATE_TIME);
    private static final SinkRecord T2P1R1 = new SinkRecord(
            "topic2", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2001, 1635461504000L,
            TimestampType.CREATE_TIME);
    private static final SinkRecord T2P1R2 = new SinkRecord(
            "topic2", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2002, 1635547904000L,
            TimestampType.CREATE_TIME);
    private static final SinkRecord T2P1R3 = new SinkRecord(
            "topic2", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2003, 1635547906000L,
            TimestampType.CREATE_TIME);

    private static final TimestampSource DEFAULT_TS_SOURCE =
        TimestampSource.of(TimestampSource.Type.WALLCLOCK);

    @Test
    void withoutNecessaryParameters() {
        assertThatThrownBy(() -> new TopicPartitionRecordGrouper(null, 0, DEFAULT_TS_SOURCE))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("filenameTemplate cannot be null");

        assertThatThrownBy(() -> new TopicPartitionRecordGrouper(Template.of("{{topic}}"), 0, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("tsSource cannot be null");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = 10)
    void empty(final Integer maxRecordsPerFile) {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, maxRecordsPerFile, DEFAULT_TS_SOURCE);
        assertThat(grouper.records()).isEmpty();
    }

    @Test
    void unlimited() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                    filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);
        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-0-0", list(T0P0R0, T0P0R1, T0P0R2, T0P0R3, T0P0R4, T0P0R5)),
                entry("topic0-1-10", list(T0P1R0, T0P1R1, T0P1R2, T0P1R3)),
                entry("topic1-1-1000", list(T1P1R0, T1P1R1, T1P1R2, T1P1R3))
            );
    }

    @Test
    void limited() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                    filenameTemplate, 2, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);
        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-0-0", list(T0P0R0, T0P0R1)),
                entry("topic0-0-2", list(T0P0R2, T0P0R3)),
                entry("topic0-0-4", list(T0P0R4, T0P0R5)),
                entry("topic0-1-10", list(T0P1R0, T0P1R1)),
                entry("topic0-1-12", list(T0P1R2, T0P1R3)),
                entry("topic1-1-1000", list(T1P1R0, T1P1R1)),
                entry("topic1-1-1002", list(T1P1R2, T1P1R3))
            );
    }

    @Test
    void clear() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                    filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T0P1R3);

        grouper.clear();
        assertThat(grouper.records()).isEmpty();

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsExactly(
                entry("topic0-0-4", list(T0P0R4, T0P0R5)),
                entry("topic1-1-1000", list(T1P1R0, T1P1R1, T1P1R2, T1P1R3))
            );
    }

    @Test
    void setZeroPaddingForKafkaOffset() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset:padding=true}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                    filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-0-00000000000000000004", list(T0P0R4, T0P0R5)),
                entry("topic1-1-00000000000000001000", list(T1P1R0, T1P1R1, T1P1R2, T1P1R3))
            );
    }

    @Test
    void setZeroPaddingForKafkaPartition() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition:padding=true}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
                new TopicPartitionRecordGrouper(
                        filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records)
            .containsOnly(
                entry("topic0-0000000000-4", list(T0P0R4, T0P0R5)),
                entry("topic1-0000000001-1000", list(T1P1R0, T1P1R1, T1P1R2, T1P1R3))
            );
    }

    @Test
    void addTimeUnitsToTheFileNameUsingWallclockTimestampSource() {
        final Template filenameTemplate =
                Template.of(
                        "{{topic}}-"
                                + "{{partition}}-"
                                + "{{start_offset}}-"
                                + "{{timestamp:unit=yyyy}}"
                                + "{{timestamp:unit=MM}}"
                                + "{{timestamp:unit=dd}}"
                );
        final ZonedDateTime t = TimestampSource.of(TimestampSource.Type.WALLCLOCK).time(null);
        final String expectedTs =
                t.format(DateTimeFormatter.ofPattern("yyyy"))
                        + t.format(DateTimeFormatter.ofPattern("MM"))
                        + t.format(DateTimeFormatter.ofPattern("dd"));

        final TopicPartitionRecordGrouper grouper =
                new TopicPartitionRecordGrouper(
                        filenameTemplate, null, TimestampSource.of(TimestampSource.Type.WALLCLOCK));

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic0-0-4-" + expectedTs, list(T0P0R4, T0P0R5)),
                entry("topic1-1-1000-" + expectedTs, list(T1P1R0, T1P1R1, T1P1R2, T1P1R3))
            );
    }

    @Test
    void rotateKeysHourly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=yyyy}}"
                    + "{{timestamp:unit=MM}}"
                    + "{{timestamp:unit=dd}}"
                    + "{{timestamp:unit=HH}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstHourTime = ZonedDateTime.now();
        final ZonedDateTime secondHourTime = firstHourTime.plusHours(1);
        final String firstHourTs =
            firstHourTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("MM"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("dd"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("HH"));
        final String secondHourTs =
            secondHourTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("MM"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("dd"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("HH"));

        when(timestampSourceMock.time(any())).thenReturn(firstHourTime);

        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate, null, timestampSourceMock);

        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);

        when(timestampSourceMock.time(any())).thenReturn(secondHourTime);

        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic0-0-1-" + firstHourTs, list(T0P0R1, T0P0R2, T0P0R3)),
                entry("topic0-0-1-" + secondHourTs, list(T0P0R4, T0P0R5))
            );
    }

    @Test
    void rotateKeysDaily() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=yyyy}}"
                    + "{{timestamp:unit=MM}}"
                    + "{{timestamp:unit=dd}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstDayTime = ZonedDateTime.now();
        final ZonedDateTime secondDayTime = firstDayTime.plusDays(1);
        final String firstDayTs =
            firstDayTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + firstDayTime.format(DateTimeFormatter.ofPattern("MM"))
                + firstDayTime.format(DateTimeFormatter.ofPattern("dd"));
        final String secondDayTs =
            secondDayTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + secondDayTime.format(DateTimeFormatter.ofPattern("MM"))
                + secondDayTime.format(DateTimeFormatter.ofPattern("dd"));

        when(timestampSourceMock.time(any())).thenReturn(firstDayTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate, null, timestampSourceMock);

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time(any())).thenReturn(secondDayTime);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic0-1-10-" + firstDayTs, list(T0P1R0, T0P1R1, T0P1R2)),
                entry("topic0-1-10-" + secondDayTs, list(T0P1R3))
            );
    }

    @Test
    void rotateKeysMonthly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=yyyy}}"
                    + "{{timestamp:unit=MM}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstMonthTime = ZonedDateTime.now().with(TemporalAdjusters.lastDayOfMonth());
        final ZonedDateTime secondMonth = firstMonthTime.plusDays(1);
        final String firstMonthTs =
            firstMonthTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + firstMonthTime.format(DateTimeFormatter.ofPattern("MM"));
        final String secondMonthTs =
            secondMonth.format(DateTimeFormatter.ofPattern("yyyy"))
                + secondMonth.format(DateTimeFormatter.ofPattern("MM"));

        when(timestampSourceMock.time(any())).thenReturn(firstMonthTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate, null, timestampSourceMock);

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time(any())).thenReturn(secondMonth);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic0-1-10-" + firstMonthTs, list(T0P1R0, T0P1R1, T0P1R2)),
                entry("topic0-1-10-" + secondMonthTs, list(T0P1R3))
            );
    }

    @Test
    void rotateKeysYearly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=yyyy}}"
                    + "{{timestamp:unit=MM}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstYearTime = ZonedDateTime.now();
        final ZonedDateTime secondYearMonth = firstYearTime.plusYears(1);
        final String firstYearTs =
            firstYearTime.format(DateTimeFormatter.ofPattern("yyyy"))
                + firstYearTime.format(DateTimeFormatter.ofPattern("MM"));
        final String secondYearTs =
            secondYearMonth.format(DateTimeFormatter.ofPattern("yyyy"))
                + secondYearMonth.format(DateTimeFormatter.ofPattern("MM"));

        when(timestampSourceMock.time(any())).thenReturn(firstYearTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate, null, timestampSourceMock);

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time(any())).thenReturn(secondYearMonth);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic0-1-10-" + firstYearTs, list(T0P1R0, T0P1R1, T0P1R2)),
                entry("topic0-1-10-" + secondYearTs, list(T0P1R3))
            );
    }

    @Test
    void rotateDailyWithEventTimestampSource() {
        final Template filenameTemplate =
                Template.of(
                        "{{topic}}-"
                                + "{{partition}}-"
                                + "{{start_offset}}-"
                                + "{{timestamp:unit=yyyy}}"
                                + "{{timestamp:unit=MM}}"
                                + "{{timestamp:unit=dd}}"
                );
        final ZonedDateTime t0 = TimestampSource.of(TimestampSource.Type.EVENT).time(T2P1R0);
        final ZonedDateTime t1 = TimestampSource.of(TimestampSource.Type.EVENT).time(T2P1R1);
        final ZonedDateTime t2 = TimestampSource.of(TimestampSource.Type.EVENT).time(T2P1R2);

        final String expectedTs0 =
                t0.format(DateTimeFormatter.ofPattern("yyyy"))
                        + t0.format(DateTimeFormatter.ofPattern("MM"))
                        + t0.format(DateTimeFormatter.ofPattern("dd"));
        final String expectedTs1 =
                t1.format(DateTimeFormatter.ofPattern("yyyy"))
                        + t1.format(DateTimeFormatter.ofPattern("MM"))
                        + t1.format(DateTimeFormatter.ofPattern("dd"));
        final String expectedTs2 =
                t2.format(DateTimeFormatter.ofPattern("yyyy"))
                        + t2.format(DateTimeFormatter.ofPattern("MM"))
                        + t2.format(DateTimeFormatter.ofPattern("dd"));

        final TopicPartitionRecordGrouper grouper =
                new TopicPartitionRecordGrouper(
                        filenameTemplate, null, TimestampSource.of(TimestampSource.Type.EVENT));

        grouper.put(T2P1R0);
        grouper.put(T2P1R1);
        grouper.put(T2P1R2);
        grouper.put(T2P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(records)
            .containsOnly(
                entry("topic2-1-2000-" + expectedTs0, list(T2P1R0)),
                entry("topic2-1-2000-" + expectedTs1, list(T2P1R1)),
                entry("topic2-1-2000-" + expectedTs2, list(T2P1R2, T2P1R3))
            );
    }
}
