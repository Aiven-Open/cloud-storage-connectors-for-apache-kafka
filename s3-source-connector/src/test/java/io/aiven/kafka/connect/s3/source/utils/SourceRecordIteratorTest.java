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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.common.source.offsets.OffsetManager;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private static final String DFLT_BUCKET = "bucket";
    private static final String DFLT_TOPIC = "topic";

    private AmazonS3 mockS3Client;
    private S3SourceConfig mockConfig;

    private List<S3ObjectSummary> s3ObjectSummaries;

    private Map<String, S3Object> s3ObjectMap;

    private String getObjectContents(final String bucket, final String key) {
        return String.format("Hello from %s on %s", key, bucket);
    }

    private void createS3Object(final S3ObjectSummary summary) {
        final S3Object result = new S3Object(); // NOPMD
        result.setKey(summary.getKey());
        result.setBucketName(summary.getBucketName());
        result.setObjectContent(new ByteArrayInputStream(
                getObjectContents(summary.getBucketName(), summary.getKey()).getBytes(StandardCharsets.UTF_8)));
        s3ObjectMap.put(summary.getKey(), result);
    }
    private S3ObjectSummary createS3ObjectSummary(final String key) {
        final S3ObjectSummary result = new S3ObjectSummary();
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
    }

    void assertS3RecordMatches(final S3SourceRecord s3SourceRecord, final String key, final String value,
            final int counter) {
        assertThat(s3SourceRecord.getRecordKey())
                .as(() -> String.format("Wrong record key at %s: %s != %s (expected)", counter,
                        new String(s3SourceRecord.getRecordKey(), StandardCharsets.UTF_8), key))
                .isEqualTo(key.getBytes(StandardCharsets.UTF_8));
        assertThat(s3SourceRecord.getRecordValue())
                .as(() -> String.format("Wrong record value at %s: %s != %s (expected)", counter,
                        new String(s3SourceRecord.getRecordValue(), StandardCharsets.UTF_8), value))
                .isEqualTo(value.getBytes(StandardCharsets.UTF_8));
    }

    private List<S3OffsetManagerEntry> multiKeySetUp(final String[] keys, final String topic, final int partition) {
        final List<S3OffsetManagerEntry> entries = new ArrayList<>();
        for (final String key : keys) {
            final S3ObjectSummary summary = createS3ObjectSummary(key);
            entries.add(new S3OffsetManagerEntry(summary.getBucketName(), summary.getKey(), topic, partition));
            s3ObjectSummaries.add(summary);
        }
        return entries;
    }

    @Test
    void allObjectsTest() {
        final String[] keys = { "topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt" };
        multiKeySetUp(keys, DFLT_TOPIC, 1);
        // create an empty offset manager.
        final OffsetManager offsetManager = new S3OffsetManager(new HashMap<>());

        final Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        final SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client,
                offsetManager, new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];

            assertThat(sourceRecordIterator).hasNext();
            final S3SourceRecord sourceRecord = sourceRecordIterator.next();
            assertS3RecordMatches(sourceRecord, key, TestingTransformer.transform(getObjectContents(DFLT_BUCKET, key)),
                    i);
        }
        assertThat(sourceRecordIterator.hasNext()).isFalse();
    }

    @Test
    void skipObjectsTest() {
        final String[] keys = { "topic-00001-key1.txt", "topic-00001-key2.txt", "topic-00001-key3.txt" };
        final List<S3OffsetManagerEntry> entries = multiKeySetUp(keys, DFLT_TOPIC, 1);
        final OffsetManager offsetManager = new S3OffsetManager(new HashMap<>());
        // set the key to skip.
        offsetManager.updateCurrentOffsets(entries.get(0));

        final Iterator<S3ObjectSummary> s3ObjectSummaryIterator = s3ObjectSummaries.iterator();

        final SourceRecordIterator sourceRecordIterator = new SourceRecordIterator(mockConfig, mockS3Client,
                offsetManager, new TestingTransformer(), s3ObjectSummaryIterator);
        for (int i = 1; i < keys.length; i++) {
            final String key = keys[i];

            assertThat(sourceRecordIterator).as("at position " + i).hasNext();
            final S3SourceRecord sourceRecord = sourceRecordIterator.next();
            assertS3RecordMatches(sourceRecord, key, TestingTransformer.transform(getObjectContents(DFLT_BUCKET, key)),
                    i);
        }

        assertThat(sourceRecordIterator.hasNext()).isFalse();
    }
}
