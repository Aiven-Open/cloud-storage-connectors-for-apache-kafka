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

package io.aiven.kafka.connect.s3.source.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import org.junit.jupiter.api.Test;

class S3SourceRecordTest {

    @Test
    void testCreateSourceRecord() {
        final Converter valueConverter = mock(Converter.class);
        final Converter keyConverter = mock(Converter.class);
        final S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "test-topic",
                1);
        final S3SourceRecord s3SourceRecord = new S3SourceRecord(offsetManagerEntry,
                "mock-key".getBytes(StandardCharsets.UTF_8), "mock-value".getBytes(StandardCharsets.UTF_8));

        when(valueConverter.toConnectData(anyString(), any()))
                .thenReturn(new SchemaAndValue(Schema.BYTES_SCHEMA, "value-converted"));

        when(keyConverter.toConnectData(anyString(), any())).thenReturn(new SchemaAndValue(null, "key-converted"));

        final SourceRecord sourceRecord = s3SourceRecord.getSourceRecord(Optional.of(keyConverter), valueConverter);
        assertThat(sourceRecord.key()).isEqualTo("key-converted");
        assertThat(sourceRecord.value()).isEqualTo("value-converted");
        assertThat(sourceRecord.sourcePartition()).isEqualTo(offsetManagerEntry.getManagerKey().getPartitionMap());
        assertThat(sourceRecord.kafkaPartition()).isEqualTo(offsetManagerEntry.getPartition());
        assertThat(sourceRecord.sourceOffset()).isEqualTo(offsetManagerEntry.getProperties());
        assertThat(sourceRecord.valueSchema()).isEqualTo(Schema.BYTES_SCHEMA);
        assertThat(sourceRecord.keySchema()).isNull();
        assertThat(sourceRecord.topic()).isEqualTo(offsetManagerEntry.getTopic());
    }
}
