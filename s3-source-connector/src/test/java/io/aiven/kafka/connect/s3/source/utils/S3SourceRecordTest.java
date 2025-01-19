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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.model.S3Object;

@ExtendWith(MockitoExtension.class)
class S3SourceRecordTest {

    public static final String TEST_OBJECT_KEY_TXT = "test-object-key.txt";
    public static final String BUCKET_ONE = "bucket-one";
    @Mock
    private S3SourceConfig s3SourceConfig;

    private Context<String> context;

    @Mock
    private S3Object s3Object;

    @Test
    void testCreateSourceRecord() {
        final String topic = "test-topic";
        final S3OffsetManagerEntry entry = new S3OffsetManagerEntry(BUCKET_ONE, TEST_OBJECT_KEY_TXT);
        context = new Context<>(TEST_OBJECT_KEY_TXT);
        context.setPartition(null);
        context.setTopic(topic);
        final S3SourceRecord s3Record = new S3SourceRecord(s3Object);
        s3Record.setOffsetManagerEntry(entry);
        s3Record.setContext(context);
        s3Record.setValueData(new SchemaAndValue(null, ""));
        s3Record.setKeyData(new SchemaAndValue(null, ""));

        final SourceRecord result = s3Record.getSourceRecord(ErrorsTolerance.NONE);

        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

    }

    @Test
    void testCreateSourceRecordWithDataError() {
        context = mock(Context.class);
        final S3OffsetManagerEntry offsetManagerEntry = mock(S3OffsetManagerEntry.class);
        when(offsetManagerEntry.getManagerKey()).thenThrow(new DataException("Test Exception"));
        final S3SourceRecord s3Record = new S3SourceRecord(s3Object);
        s3Record.setOffsetManagerEntry(offsetManagerEntry);
        s3Record.setContext(context);

        assertThatExceptionOfType(ConnectException.class).as("Errors tolerance: NONE")
                .isThrownBy(() -> s3Record.getSourceRecord(ErrorsTolerance.NONE));
        final SourceRecord result = s3Record.getSourceRecord(ErrorsTolerance.ALL);
        assertThat(result).isNull();
    }

}
