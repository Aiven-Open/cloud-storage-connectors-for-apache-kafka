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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Iterator;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordProcessorTest {

    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private Converter valueConverter;
    @Mock
    private Transformer transformer;
    @Mock
    private Converter keyConverter;
    @Mock
    private OffsetManager offsetManager;

    @Mock
    private AWSV2SourceClient sourceClient;

    private static final Supplier<Boolean> TRUE = () -> true;
    private static final Supplier<Boolean> FALSE = () -> false;
    private Iterator<S3SourceRecord> sourceRecordIterator;

    @BeforeEach
    void setUp() {
        sourceRecordIterator = mock(Iterator.class);
    }

    @Test
    void testCreateSourceRecord() throws ConnectException {

        final SourceRecord mockSourceRecord = mock(SourceRecord.class);
        final S3SourceRecord mockRecord = mock(S3SourceRecord.class);
        when(mockRecord.getSourceRecord(any())).thenReturn(mockSourceRecord);

        SourceRecord result = RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient,
                offsetManager);

        verify(offsetManager, times(1)).updateCurrentOffsets(any(), any());
        verify(mockRecord, times(2)).getPartitionMap();
        assertThat(result).isEqualTo(mockSourceRecord);

    }

    @Test
    void testCreateSourceRecordWithDataError() throws ConnectException {

        final S3SourceRecord mockRecord = mock(S3SourceRecord.class);
        when(mockRecord.getSourceRecord(any())).thenThrow(new DataException("Testing exception"));

        when(s3SourceConfig.getErrorsTolerance()).thenReturn(ErrorsTolerance.NONE);

        assertThatExceptionOfType(ConnectException.class).as("Errors tolerance: NONE")
                .isThrownBy(() -> RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient,
                        offsetManager));

        when(s3SourceConfig.getErrorsTolerance()).thenReturn(ErrorsTolerance.ALL);
        SourceRecord result = RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient,
                offsetManager);
        assertThat(result).isNull();
    }

    @Test
    void testCreateSourceRecords() {
        final S3SourceRecord mockRecord = mock(S3SourceRecord.class);
        when(mockRecord.getSourceRecord(any())).thenReturn(mock(SourceRecord.class));

        final SourceRecord sourceRecords = RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient,
                offsetManager);

        assertThat(sourceRecords).isNotNull();
    }

    @Test
    void errorToleranceOnNONE() {
        final S3SourceRecord mockRecord = mock(S3SourceRecord.class);
        when(mockRecord.getSourceRecord(any())).thenThrow(new DataException("generic issue"));

        when(s3SourceConfig.getErrorsTolerance()).thenReturn(ErrorsTolerance.NONE);

        assertThatThrownBy(
                () -> RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient, offsetManager))
                .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
                .hasMessage("Data Exception caught during S3 record to source record transformation");

    }

    @Test
    void errorToleranceOnALL() {
        final S3SourceRecord mockRecord = mock(S3SourceRecord.class);
        when(mockRecord.getSourceRecord(any())).thenThrow(new DataException("generic issue"));

        when(s3SourceConfig.getErrorsTolerance()).thenReturn(ErrorsTolerance.ALL);

        assertThat(RecordProcessor.createSourceRecord(mockRecord, s3SourceConfig, sourceClient, offsetManager))
                .isNull();

    }
}
