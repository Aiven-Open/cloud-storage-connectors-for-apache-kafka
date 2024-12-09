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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private S3SourceConfig mockConfig;
    private OffsetManager mockOffsetManager;
    private Transformer mockTransformer;

    private AWSV2SourceClient mockSourceApiClient;

    @BeforeEach
    public void setUp() {
        mockConfig = mock(S3SourceConfig.class);
        mockOffsetManager = mock(OffsetManager.class);
        mockTransformer = mock(Transformer.class);
        mockSourceApiClient = mock(AWSV2SourceClient.class);
    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {

        final String key = "topic-00001-abc123.txt";

        // Mock S3Object and InputStream
        try (S3Object mockS3Object = mock(S3Object.class);
                S3ObjectInputStream mockInputStream = new S3ObjectInputStream(new ByteArrayInputStream(new byte[] {}),
                        null);) {
            when(mockSourceApiClient.getObject(anyString())).thenReturn(mockS3Object);
            when(mockS3Object.getObjectContent()).thenReturn(mockInputStream);

            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any())).thenReturn(Stream.of(new Object()));

            final String outStr = "this is a test";
            when(mockTransformer.getValueBytes(any(), anyString(), any()))
                    .thenReturn(outStr.getBytes(StandardCharsets.UTF_8));

            when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());

            when(mockSourceApiClient.getListOfObjectKeys(any())).thenReturn(Collections.emptyIterator());
            SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
                    mockSourceApiClient);

            assertThat(iterator.hasNext()).isFalse();
            assertThat(iterator.next()).isNull();

            when(mockSourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());

            iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer, mockSourceApiClient);

            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isNotNull();
        }
    }
}
