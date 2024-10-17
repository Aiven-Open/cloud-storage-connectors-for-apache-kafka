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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private AmazonS3 mockS3Client;
    private S3SourceConfig mockConfig;
    private OffsetManager mockOffsetManager;
    private Transformer mockTransformer;

    private FileReader mockFileReader;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3.class);
        mockConfig = mock(S3SourceConfig.class);
        mockOffsetManager = mock(OffsetManager.class);
        mockTransformer = mock(Transformer.class);
        mockFileReader = mock(FileReader.class);
    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {
        final S3ObjectSummary mockSummary = new S3ObjectSummary();
        mockSummary.setKey("topic-00001-abc123.txt");

        // Mock list of S3 object summaries
        final List<S3ObjectSummary> mockObjectSummaries = Collections.singletonList(mockSummary);

        final ListObjectsV2Result result = mockListObjectsResult(mockObjectSummaries);
        when(mockS3Client.listObjectsV2(anyString())).thenReturn(result);

        // Mock S3Object and InputStream
        try (S3Object mockS3Object = mock(S3Object.class);
                S3ObjectInputStream mockInputStream = new S3ObjectInputStream(new ByteArrayInputStream(new byte[] {}),
                        null);) {
            when(mockS3Client.getObject(anyString(), anyString())).thenReturn(mockS3Object);
            when(mockS3Object.getObjectContent()).thenReturn(mockInputStream);

            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any()))
                    .thenReturn(Collections.singletonList(new Object()));

            final String outStr = "this is a test";
            when(mockTransformer.getValueBytes(any(), anyString(), any()))
                    .thenReturn(outStr.getBytes(StandardCharsets.UTF_8));

            when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());

            when(mockFileReader.fetchObjectSummaries(any())).thenReturn(Collections.emptyIterator());
            SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockS3Client, "test-bucket",
                    mockOffsetManager, mockTransformer, mockFileReader);

            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            when(mockFileReader.fetchObjectSummaries(any())).thenReturn(mockObjectSummaries.listIterator());

            iterator = new SourceRecordIterator(mockConfig, mockS3Client, "test-bucket", mockOffsetManager,
                    mockTransformer, mockFileReader);

            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }

    }

    private ListObjectsV2Result mockListObjectsResult(final List<S3ObjectSummary> summaries) {
        final ListObjectsV2Result result = mock(ListObjectsV2Result.class);
        when(result.getObjectSummaries()).thenReturn(summaries);
        return result;
    }
}
