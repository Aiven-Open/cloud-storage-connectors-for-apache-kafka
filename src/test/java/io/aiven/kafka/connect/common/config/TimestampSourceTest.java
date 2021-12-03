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

package io.aiven.kafka.connect.common.config;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource.FieldNameTimeStampSource;
import io.aiven.kafka.connect.common.config.TimestampSource.Type;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampSourceTest {
    final String partitionFieldName = "event.occurrence.timestamp";

    @Test
    void fieldNamePartitionExceptions() {

        final Schema schemaWithOutPartitionField = SchemaBuilder.struct()
            .name("com.example.no_partition_field").version(1)
            .field("month", Schema.STRING_SCHEMA)
            .build();

        final TimestampSource timestampSourceFieldName = TimestampSource.of(Type.PARTITION_FIELDNAME);

        final Struct value = new Struct(schemaWithOutPartitionField);
        value.put("month", "January");

        final SinkRecord sinkRecordNoPartitionField = new SinkRecord(
            "topic0", 0,
            SchemaBuilder.string().optional().version(1).build(), "some_key",
            schemaWithOutPartitionField, value, 1);

        // partition field not set, exception is expected here
        assertThrows(DataException.class, () -> {
            timestampSourceFieldName.time(Optional.of(sinkRecordNoPartitionField));
        });

        ((FieldNameTimeStampSource) timestampSourceFieldName).setPartitionFieldName(partitionFieldName);
        // partition field is set, but the field doesn't exist
        assertThrows(DataException.class, () -> {
            timestampSourceFieldName.time(Optional.of(sinkRecordNoPartitionField));
        });
    }

    @Test
    void fieldNamePartition() {

        final Schema schemaWithPartitionField = SchemaBuilder.struct()
            .name("com.example.with_partition_field").version(1)
            .field("month", Schema.STRING_SCHEMA)
            .field(partitionFieldName, Schema.INT64_SCHEMA)
            .build();

        final TimestampSource timestampSourceFieldName = TimestampSource.of(Type.PARTITION_FIELDNAME);
        ((FieldNameTimeStampSource) timestampSourceFieldName).setPartitionFieldName(partitionFieldName);

        final Long eventOccurrenceTimestamp = 1637035167540L;
        final ZonedDateTime expectedDatetime = ZonedDateTime
            .ofInstant(Instant.ofEpochMilli(eventOccurrenceTimestamp), ZoneOffset.UTC);

        final Struct value = new Struct(schemaWithPartitionField);
        value.put("month", "January");
        value.put(partitionFieldName, eventOccurrenceTimestamp);

        final SinkRecord sinkRecordWithPartitionField = new SinkRecord(
            "topic1", 0,
            SchemaBuilder.string().optional().version(1).build(), "some_key",
            schemaWithPartitionField, value, 2);

        final ZonedDateTime actualDateTime = timestampSourceFieldName.time(Optional.of(sinkRecordWithPartitionField));

        assertEquals(actualDateTime, expectedDatetime);
    }
}
