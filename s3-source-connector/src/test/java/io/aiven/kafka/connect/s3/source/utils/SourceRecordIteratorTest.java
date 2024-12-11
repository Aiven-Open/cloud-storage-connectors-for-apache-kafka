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

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.BYTES_TRANSFORMATION_NUM_OF_RECS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils;
import io.aiven.kafka.connect.s3.source.testutils.TestingTransformer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.Test;

final class SourceRecordIteratorTest {

    private final static String BUCKET_NAME = "record-test-bucket";

    private S3SourceConfig s3SourceConfig;
    private OffsetManager offsetManager;
    private Transformer transformer;

    private AmazonS3 s3Client;

    private AWSV2SourceClient sourceClient;

    private Map<String, String> standardConfigurationData() {
        return Map.of(AWS_S3_BUCKET_NAME_CONFIG, BUCKET_NAME, "tasks.max", "1", "task.id", "0");
    }

    private S3Object addInputStream(final S3Object s3Object, final byte[] data) {
        final S3ObjectInputStream stream = new S3ObjectInputStream(new ByteArrayInputStream(data),
                mock(HttpRequestBase.class), false);
        s3Object.setObjectContent(stream);
        return s3Object;
    }

    @Test
    void testSingleS3Object() throws IOException {
        s3SourceConfig = new S3SourceConfig(standardConfigurationData());
        offsetManager = mock(OffsetManager.class);
        when(offsetManager.recordsProcessedForObjectKey(anyMap(), anyString())).thenReturn(0L);
        transformer = new TestingTransformer();
        s3Client = mock(AmazonS3.class);

        final String key = "topic-00001-abc123.txt";
        final S3ObjectSummary summary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, key);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(summary), null);
        // create S3Object with content.
        try (S3Object s3Object = addInputStream(S3ObjectsUtils.createS3Object(summary),
                "Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(s3Client.getObject(summary.getBucketName(), summary.getKey())).thenReturn(s3Object);
            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                    .thenReturn(new ListObjectsV2Result());
            sourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());

            final SourceRecordIterator underTest = new SourceRecordIterator(s3SourceConfig, offsetManager, transformer,
                    sourceClient);

            assertThat(underTest).hasNext();
            final S3SourceRecord record = underTest.next();
            assertThat(record.getObjectKey()).isEqualTo(key);
            assertThat(record.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("TESTING: Hello World".getBytes(StandardCharsets.UTF_8));
            assertThat(record.getTopic()).isEqualTo("topic");
            assertThat(record.partition()).isEqualTo(1);

            assertThat(underTest).isExhausted();
        }
    }

    @Test
    void testMultiple3Object() throws IOException {
        s3SourceConfig = new S3SourceConfig(standardConfigurationData());
        offsetManager = mock(OffsetManager.class);
        when(offsetManager.recordsProcessedForObjectKey(anyMap(), anyString())).thenReturn(0L);

        transformer = new TestingTransformer();
        s3Client = mock(AmazonS3.class);

        final List<S3ObjectSummary> summaries = new ArrayList<>();

        final String key1 = "topic-00001-abc123.txt";
        final S3ObjectSummary summary1 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, key1);
        summaries.add(summary1);

        final String key2 = "topic-00002-abc123.txt";
        final S3ObjectSummary summary2 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, key2);
        summaries.add(summary2);

        try (S3Object s3Object1 = addInputStream(S3ObjectsUtils.createS3Object(summary1),
                "Hello World1".getBytes(StandardCharsets.UTF_8));
                S3Object s3Object2 = addInputStream(S3ObjectsUtils.createS3Object(summary2),
                        "Hello World2".getBytes(StandardCharsets.UTF_8))) {

            when(s3Client.getObject(summary1.getBucketName(), summary1.getKey())).thenReturn(s3Object1);

            when(s3Client.getObject(summary2.getBucketName(), summary2.getKey())).thenReturn(s3Object2);

            final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils.createListObjectsV2Result(summaries, null);

            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                    .thenReturn(new ListObjectsV2Result());
            sourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());

            final SourceRecordIterator underTest = new SourceRecordIterator(s3SourceConfig, offsetManager, transformer,
                    sourceClient);

            assertThat(underTest).hasNext();
            S3SourceRecord record = underTest.next();
            assertThat(record.getObjectKey()).isEqualTo(key1);
            assertThat(record.key()).isEqualTo(key1.getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("TESTING: Hello World1".getBytes(StandardCharsets.UTF_8));
            assertThat(record.getTopic()).isEqualTo("topic");
            assertThat(record.partition()).isEqualTo(1);

            assertThat(underTest).hasNext();
            record = underTest.next();
            assertThat(record.getObjectKey()).isEqualTo(key2);
            assertThat(record.key()).isEqualTo(key2.getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("TESTING: Hello World2".getBytes(StandardCharsets.UTF_8));
            assertThat(record.getTopic()).isEqualTo("topic");
            assertThat(record.partition()).isEqualTo(2);

            assertThat(underTest).isExhausted();
        }
    }

    @Test
    void testByteArrayProcessorSkipsProcessedRecords() throws Exception {

        s3SourceConfig = new S3SourceConfig(standardConfigurationData());
        offsetManager = mock(OffsetManager.class);
        when(offsetManager.recordsProcessedForObjectKey(anyMap(), anyString()))
                .thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);

        transformer = mock(ByteArrayTransformer.class);
        s3Client = mock(AmazonS3.class);

        final String key = "topic-00001-abc123.txt";
        final S3ObjectSummary summary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, key);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(summary), null);
        // create S3Object with content.
        try (S3Object s3Object = addInputStream(S3ObjectsUtils.createS3Object(summary),
                "Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(s3Client.getObject(summary.getBucketName(), summary.getKey())).thenReturn(s3Object);
            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                    .thenReturn(new ListObjectsV2Result());
            sourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());

            final SourceRecordIterator underTest = new SourceRecordIterator(s3SourceConfig, offsetManager, transformer,
                    sourceClient);

            assertThat(underTest).isExhausted();
            verify(transformer, times(0)).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }
    }

    @Test
    void testMultipleRecordSkipRecords() throws Exception {

        s3SourceConfig = new S3SourceConfig(standardConfigurationData());
        offsetManager = mock(OffsetManager.class);
        when(offsetManager.recordsProcessedForObjectKey(anyMap(), anyString())).thenReturn(1L);

        transformer = new TestingTransformer();
        s3Client = mock(AmazonS3.class);

        final String key = "topic-00001-abc123.txt";
        final S3ObjectSummary summary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, key);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(summary), null);
        // create S3Object with content.
        try (S3Object s3Object = addInputStream(S3ObjectsUtils.createS3Object(summary),
                String.format("Hello World%nIt is nice to see you%nGoodbye cruel world")
                        .getBytes(StandardCharsets.UTF_8))) {
            when(s3Client.getObject(summary.getBucketName(), summary.getKey())).thenReturn(s3Object);
            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                    .thenReturn(new ListObjectsV2Result());
            sourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());

            final SourceRecordIterator underTest = new SourceRecordIterator(s3SourceConfig, offsetManager, transformer,
                    sourceClient);

            assertThat(underTest).hasNext();
            S3SourceRecord record = underTest.next();
            assertThat(record.getObjectKey()).isEqualTo(key);
            assertThat(record.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("TESTING: It is nice to see you".getBytes(StandardCharsets.UTF_8));
            assertThat(record.getTopic()).isEqualTo("topic");
            assertThat(record.partition()).isEqualTo(1);

            assertThat(underTest).hasNext();
            record = underTest.next();
            assertThat(record.getObjectKey()).isEqualTo(key);
            assertThat(record.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("TESTING: Goodbye cruel world".getBytes(StandardCharsets.UTF_8));
            assertThat(record.getTopic()).isEqualTo("topic");
            assertThat(record.partition()).isEqualTo(1);

            assertThat(underTest).isExhausted();
        }
    }

    @Test
    void fileNameMatcherFilterTest() {
        final SourceRecordIterator.FileMatcherFilter underTest = new SourceRecordIterator.FileMatcherFilter();
        assertThat(underTest.getPartitionId()).isEqualTo(-1);
        assertThat(underTest.getTopicName()).isNull();

        assertThat(underTest.test(createObjectSummary("topic-00001-abc123.txt"))).isTrue();
        assertThat(underTest.getPartitionId()).isEqualTo(1);
        assertThat(underTest.getTopicName()).isEqualTo("topic");

        assertThat(underTest.test(createObjectSummary("invalidFileName"))).isFalse();
        assertThat(underTest.getPartitionId()).isEqualTo(-1);
        assertThat(underTest.getTopicName()).isNull();
    }

    @Test
    void taskAssignmentFilterTest() {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("tasks.max", "4");
        configMap.put("task.id", "2");
        configMap.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket");
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configMap);
        final SourceRecordIterator.TaskAssignmentFilter underTest = new SourceRecordIterator.TaskAssignmentFilter(
                s3SourceConfig);

        // key1 with tasks.max = 4 will return task 2.
        assertThat(underTest.test(createObjectSummary("key1"))).isTrue();
        assertThat(underTest.test(createObjectSummary("key2"))).isFalse();
    }

    private S3ObjectSummary createObjectSummary(final String objectKey) {
        final S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(objectKey);
        return summary;
    }
}
