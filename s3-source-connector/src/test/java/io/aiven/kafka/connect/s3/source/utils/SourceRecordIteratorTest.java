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

import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.RECORD_COUNT;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.BYTES_TRANSFORMATION_NUM_OF_RECS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

final class SourceRecordIteratorTest {

    private S3SourceConfig mockConfig;
    private S3OffsetManagerEntry mockS3OffsetManagerEntry;
    private OffsetManager<S3OffsetManagerEntry> mockOffsetManager;
    private Transformer mockTransformer;

    private AWSV2SourceClient mockSourceApiClient;

    @BeforeEach
    public void setUp() {
        mockConfig = mock(S3SourceConfig.class);
        mockOffsetManager = mock(OffsetManager.class);
        mockS3OffsetManagerEntry = mock(S3OffsetManagerEntry.class);
        mockTransformer = mock(Transformer.class);
        mockSourceApiClient = mock(AWSV2SourceClient.class);
        when(mockConfig.getAwsS3BucketName()).thenReturn("bucket_name");
    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {

        final String key = "topic-00001-abc123.txt";

        // Mock InputStream
        try (InputStream mockInputStream = new ByteArrayInputStream(new byte[] {})) {
            when(mockSourceApiClient.getObject(anyString())).thenReturn(() -> mockInputStream);

            mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);

            when(mockS3OffsetManagerEntry.getProperties()).thenReturn(Collections.emptyMap());

            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Collections.emptyIterator());
            Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
                    mockSourceApiClient);

            assertThat(iterator.hasNext()).isFalse();

            final S3Object obj = S3Object.builder().key(key).build();

            final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
            when(mockSourceApiClient.getObject(any())).thenReturn(() -> bais);
            iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer, mockSourceApiClient);

            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isNotNull();
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {

        final String key = "topic-00001-abc123.txt";
        final S3Object s3Object = S3Object.builder().key(key).build();

        // With ByteArrayTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(mockSourceApiClient.getObject(key)).thenReturn(() -> inputStream);

            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());

            mockTransformer = mock(ByteArrayTransformer.class);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Stream.of(SchemaAndValue.NULL));

            when(mockOffsetManager.getEntry(any(), any())).thenReturn(Optional.of(mockS3OffsetManagerEntry));

            when(mockSourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());
            when(mockS3OffsetManagerEntry.getRecordCount()).thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);

            // should skip if any records were produced by source record iterator.
            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                    mockTransformer, mockSourceApiClient);
            assertThat(iterator.hasNext()).isFalse();
            verify(mockSourceApiClient, never()).getObject(any());
            verify(mockTransformer, never()).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }

        // With AvroTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {

            when(mockSourceApiClient.getObject(key)).thenReturn(() -> inputStream);
            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());
            when(mockSourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());

            final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
            when(offsetStorageReader.offset(any(Map.class))).thenReturn(Map.of(RECORD_COUNT, 1));

            final SourceTaskContext context = mock(SourceTaskContext.class);
            when(context.offsetStorageReader()).thenReturn(offsetStorageReader);

            mockOffsetManager = new OffsetManager(context);

            mockTransformer = mock(Transformer.class);
            final SchemaAndValue schemaKey = new SchemaAndValue(null, "KEY");
            final SchemaAndValue schemaValue = new SchemaAndValue(null, "VALUE");
            when(mockTransformer.getKeyData(anyString(), anyString(), any())).thenReturn(schemaKey);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Arrays.asList(schemaValue).stream());

            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                    mockTransformer, mockSourceApiClient);
            assertThat(iterator.hasNext()).isTrue();
            final S3SourceRecord record = iterator.next();
            assertThat(record.getValue().value()).isEqualTo("VALUE");
            assertThat(record.getOffsetManagerEntry().getRecordCount()).isEqualTo(2);
            verify(mockTransformer, times(1)).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }
    }
}
