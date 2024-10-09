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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RecordProcessorTest {

    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private Converter valueConverter;
    @Mock
    private OutputWriter outputWriter;
    @Mock
    private Converter keyConverter;

    private AtomicBoolean connectorStopped;
    private Iterator<List<AivenS3SourceRecord>> sourceRecordIterator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        connectorStopped = new AtomicBoolean(false);
        sourceRecordIterator = mock(Iterator.class);
    }

    @Test
    void testProcessRecordsNoRecords() {
        when(s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS)).thenReturn(5);
        when(sourceRecordIterator.hasNext()).thenReturn(false);

        final List<SourceRecord> results = new ArrayList<>();
        final List<SourceRecord> processedRecords = RecordProcessor.processRecords(
            sourceRecordIterator,
            results,
            s3SourceConfig,
            Optional.of(keyConverter),
            valueConverter,
            connectorStopped,
            outputWriter
        );

        assertTrue(processedRecords.isEmpty(), "Processed records should be empty when there are no records.");
    }

    @Test
    void testProcessRecordsWithRecords() {
        when(s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS)).thenReturn(5);
        when(sourceRecordIterator.hasNext()).thenReturn(true, false); // One iteration with records

        final AivenS3SourceRecord mockRecord = mock(AivenS3SourceRecord.class);
        final List<AivenS3SourceRecord> recordList = Collections.singletonList(mockRecord);
        when(sourceRecordIterator.next()).thenReturn(recordList);

        final List<SourceRecord> results = new ArrayList<>();
        RecordProcessor.processRecords(
            sourceRecordIterator,
            results,
            s3SourceConfig,
            Optional.of(keyConverter),
            valueConverter,
            connectorStopped,
            outputWriter
        );

        assertThat(results.size()).isEqualTo(1);
        verify(sourceRecordIterator, times(1)).next();
    }

    @Test
    void testProcessRecordsConnectorStopped() {
        when(s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS)).thenReturn(5);
        connectorStopped.set(true); // Simulate connector stopped

        final List<SourceRecord> results = new ArrayList<>();
        final List<SourceRecord> processedRecords = RecordProcessor.processRecords(
            sourceRecordIterator,
            results,
            s3SourceConfig,
            Optional.of(keyConverter),
            valueConverter,
            connectorStopped,
            outputWriter
        );

        assertTrue(processedRecords.isEmpty(), "Processed records should be empty when connector is stopped.");
        verify(sourceRecordIterator, never()).next();
    }

    @Test
    void testCreateSourceRecords() {
        final AivenS3SourceRecord mockRecord = mock(AivenS3SourceRecord.class);
        when(mockRecord.getToTopic()).thenReturn("test-topic");
        when(mockRecord.key()).thenReturn("mock-key".getBytes(StandardCharsets.UTF_8));
        when(mockRecord.value()).thenReturn("mock-value".getBytes(StandardCharsets.UTF_8));

        when(valueConverter.toConnectData(anyString(), any()))
                .thenReturn(new SchemaAndValue(null, "mock-value-converted"));
        when(mockRecord.getSourceRecord(anyString(), any(), any())).thenReturn(mock(SourceRecord.class));

        final List<AivenS3SourceRecord> recordList = Collections.singletonList(mockRecord);
        final List<SourceRecord> sourceRecords = RecordProcessor.createSourceRecords(recordList, s3SourceConfig,
                Optional.of(keyConverter), valueConverter, new HashMap<>(), outputWriter);

        assertThat(sourceRecords.size()).isEqualTo(1);
    }
}
