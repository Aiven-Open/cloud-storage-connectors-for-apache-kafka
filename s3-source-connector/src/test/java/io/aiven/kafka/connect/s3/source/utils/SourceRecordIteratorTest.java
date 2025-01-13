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

import static io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils.PATTERN_PARTITION_KEY;
import static io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils.PATTERN_TOPIC_KEY;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.BYTES_TRANSFORMATION_NUM_OF_RECS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
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
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.input.AvroTransformer;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.HashDistributionStrategy;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.s3.model.S3Object;

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

        // Mock InputStream
        try (InputStream mockInputStream = new ByteArrayInputStream(new byte[] {})) {
            when(mockSourceApiClient.getObject(anyString())).thenReturn(() -> mockInputStream);

            mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);

            when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
            final Pattern filePattern = mock(Pattern.class);

            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Collections.emptyIterator());
            Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
                    mockSourceApiClient, new HashDistributionStrategy(1),
                    FilePatternUtils.configurePattern("{{topic}}-{{partition}}-{{start_offset}}"), 0);

            assertThat(iterator.hasNext()).isFalse();
            mockPatternMatcher(filePattern);

            final S3Object obj = S3Object.builder().key(key).build();

            final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
            when(mockSourceApiClient.getObject(any())).thenReturn(() -> bais);
            iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer, mockSourceApiClient,
                    new HashDistributionStrategy(1), filePattern, 0);

            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isNotNull();
        }
    }

    @Test
    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {
        final String key = "topic-00001-abc123.txt";
        final S3Object s3Object = S3Object.builder().key(key).build();

        // With ByteArrayTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(mockSourceApiClient.getObject(key)).thenReturn(() -> inputStream);
            final Pattern filePattern = mock(Pattern.class);

            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());

            mockTransformer = mock(ByteArrayTransformer.class);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Stream.of(SchemaAndValue.NULL));

            when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());

            when(mockSourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());
            when(mockOffsetManager.recordsProcessedForObjectKey(anyMap(), anyString()))
                    .thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);
            mockPatternMatcher(filePattern);

            // should skip if any records were produced by source record iterator.
            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                    mockTransformer, mockSourceApiClient, new HashDistributionStrategy(1), filePattern, 0);
            assertThat(iterator.hasNext()).isFalse();
            verify(mockSourceApiClient, never()).getObject(any());
            verify(mockTransformer, never()).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }

        // With AvroTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(mockSourceApiClient.getObject(key)).thenReturn(() -> inputStream);
            final Pattern filePattern = mock(Pattern.class);
            when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());
            mockTransformer = mock(AvroTransformer.class);
            when(mockSourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());

            when(mockOffsetManager.recordsProcessedForObjectKey(anyMap(), anyString()))
                    .thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);
            mockPatternMatcher(filePattern);

            when(mockTransformer.getKeyData(anyString(), anyString(), any())).thenReturn(SchemaAndValue.NULL);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Arrays.asList(SchemaAndValue.NULL).stream());

            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                    mockTransformer, mockSourceApiClient, new HashDistributionStrategy(1), filePattern, 0);
            assertThat(iterator.hasNext()).isFalse();

            verify(mockTransformer, times(0)).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }
    }

    @ParameterizedTest
    @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
            final String objectKey) {

        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
        final Pattern filePattern = mock(Pattern.class);

        mockPatternMatcher(filePattern);

        final S3Object obj = S3Object.builder().key(objectKey).build();

        final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
        when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
        when(mockSourceApiClient.getObject(any())).thenReturn(() -> bais);
        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
                mockSourceApiClient, new HashDistributionStrategy(maxTasks), filePattern, taskId);
        final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.isFileMatchingPattern(s3Object)
                && iterator.isFileAssignedToTask(s3Object);
        // Assert
        assertThat(s3ObjectPredicate).accepts(obj);
    }

    @ParameterizedTest
    @CsvSource({ "4, 1, topic1-2-0", "4, 3, key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1, key3",
            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
            final String objectKey) {
        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
        final Pattern filePattern = mock(Pattern.class);

        mockPatternMatcher(filePattern);

        final S3Object obj = S3Object.builder().key(objectKey).build();

        final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
        when(mockSourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
        when(mockSourceApiClient.getObject(any())).thenReturn(() -> bais);
        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
                mockSourceApiClient, new HashDistributionStrategy(maxTasks), filePattern, taskId);
        final Predicate<S3Object> stringPredicate = s3Object -> iterator.isFileMatchingPattern(s3Object)
                && iterator.isFileAssignedToTask(s3Object);
        // Assert
        assertThat(stringPredicate.test(obj)).as("Predicate should accept the objectKey: " + objectKey).isFalse();
    }

    private static void mockPatternMatcher(final Pattern filePattern) {
        final Matcher fileMatcher = mock(Matcher.class);
        when(filePattern.matcher(any())).thenReturn(fileMatcher);
        when(fileMatcher.find()).thenReturn(true);
        when(fileMatcher.group(PATTERN_TOPIC_KEY)).thenReturn("testtopic");
        when(fileMatcher.group(PATTERN_PARTITION_KEY)).thenReturn("0");
    }
}
