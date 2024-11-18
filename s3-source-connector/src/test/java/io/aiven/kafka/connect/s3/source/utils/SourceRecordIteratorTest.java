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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private static final String DFLT_BUCKET = "bucket";
    private static final String DFLT_TOPIC = "topic";

    private AmazonS3 mockS3Client;
    private S3SourceConfig mockConfig;
    private Transformer mockTransformer;

    private FileReader mockFileReader;

    private OffsetManager mockOffsetManager;

    private List<S3ObjectSummary> s3ObjectSummaries;

    private Map<String, S3Object> s3ObjectMap;

    private String getObjectContents(String bucket, String key) {
        return String.format("Hello from %s on %s", key, bucket);
    }

    private void createS3Object(S3ObjectSummary summary) {
        S3Object result = new S3Object();
        result.setKey(summary.getKey());
        result.setBucketName(summary.getBucketName());
        result.setObjectContent(new ByteArrayInputStream(
                getObjectContents(summary.getBucketName(), summary.getKey()).getBytes(StandardCharsets.UTF_8)));
        s3ObjectMap.put(summary.getKey(), result);
    }
    private S3ObjectSummary createS3ObjectSummary(String key) {
        S3ObjectSummary result = new S3ObjectSummary();
        result.setKey(key);
        result.setBucketName(DFLT_BUCKET);
        createS3Object(result);
        return result;
    }

    @BeforeEach
    public void setUp() {
        s3ObjectSummaries = new ArrayList<>();
        s3ObjectMap = new HashMap<>();
        mockS3Client = mock(AmazonS3.class);
        when(mockS3Client.getObject(anyString(), anyString()))
                .thenAnswer(invocation -> s3ObjectMap.get(invocation.getArguments()[1]));
        mockConfig = mock(S3SourceConfig.class);
        mockTransformer = mock(Transformer.class);
        mockOffsetManager = mock(OffsetManager.class);
    }

    void assertS3RecordMatches(S3SourceRecord s3SourceRecord, String key, String value, int counter) {
        assertThat(s3SourceRecord.getRecordKey())
                .as(() -> String.format("Wrong record key at %s: %s != %s (expected)", counter,
                        new String(s3SourceRecord.getRecordKey()), key))
                .isEqualTo(key.getBytes(StandardCharsets.UTF_8));
        assertThat(s3SourceRecord.getRecordValue())
                .as(() -> String.format("Wrong record value at %s: %s != %s (expected)", counter,
                        new String(s3SourceRecord.getRecordValue()), value))
                .isEqualTo(value.getBytes(StandardCharsets.UTF_8));
    }

    private List<S3OffsetManagerEntry> multiKeySetUp(String[] keys, String topic, int partition) {
        List<S3OffsetManagerEntry> entries = new ArrayList<>();
        for (String key : keys) {
            S3ObjectSummary summary = createS3ObjectSummary(key);
            entries.add(new S3OffsetManagerEntry(summary.getBucketName(), summary.getKey(), topic, partition));
            s3ObjectSummaries.add(summary);
        }
        return entries;
    }

    @Test
    void allObjectsTest() throws Exception {
        String[] keys = { "topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt" };
        multiKeySetUp(keys, DFLT_TOPIC, 1);
        // create an empty offset manager.
        OffsetManager offsetManager = new OffsetManager(new HashMap<>());

        Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client, offsetManager,
                new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];

            // Mock S3Object and InputStream
            try (S3Object mockS3Object = mock(S3Object.class);
                    S3ObjectInputStream mockInputStream = new S3ObjectInputStream(
                            new ByteArrayInputStream(new byte[] {}), null);) {
                when(mockS3Client.getObject(anyString(), anyString())).thenReturn(mockS3Object);
                when(mockS3Object.getObjectContent()).thenReturn(mockInputStream);

                when(mockTransformer.getRecords(any(), anyString(), anyInt(), any()))
                        .thenReturn(Stream.of(new Object()));

                final String outStr = "this is a test";
                when(mockTransformer.getValueBytes(any(), anyString(), any()))
                        .thenReturn(outStr.getBytes(StandardCharsets.UTF_8));

                when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());

                when(mockFileReader.fetchObjectSummaries(any())).thenReturn(Collections.emptyIterator());
                SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockS3Client, mockOffsetManager,
                        mockTransformer, s3ObjectSummaryIterator);

                assertThat(iterator.hasNext()).isFalse();
                assertThat(iterator.next()).isNull();

                // when(mockFileReader.fetchObjectSummaries(any())).thenReturn(mockObjectSummaries.listIterator());

                iterator = new SourceRecordIterator(mockConfig, mockS3Client, mockOffsetManager, mockTransformer,
                        s3ObjectSummaryIterator);

                assertThat(iterator.hasNext()).isTrue();
                assertThat(iterator.next()).isNotNull();

            }
            assertThat(sourceRecordIterator.hasNext()).isFalse();
        }
    }

    @Test
    void skipObjectsTest() throws Exception {
        String[] keys = { "topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt" };
        List<S3OffsetManagerEntry> entries = multiKeySetUp(keys, DFLT_TOPIC, 1);
        OffsetManager offsetManager = new OffsetManager(new HashMap<>());
        offsetManager.getOffsetValueMap("topic-00001-key1.txt",0);
        // set the key to skip.
//        offsetManager.updateCurrentOffsets(entries.get(0));

        Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client, offsetManager,
                new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 1; i < keys.length; i++) {
            String key = keys[i];

            assertThat(sourceRecordIterator).as("at position " + i).hasNext();
            S3SourceRecord sourceRecord = sourceRecordIterator.next();
            assertS3RecordMatches(sourceRecord, key, TestingTransformer.transform(getObjectContents(DFLT_BUCKET, key)),
                    i);
        }

        assertThat(sourceRecordIterator.hasNext()).isFalse();
    }
}
