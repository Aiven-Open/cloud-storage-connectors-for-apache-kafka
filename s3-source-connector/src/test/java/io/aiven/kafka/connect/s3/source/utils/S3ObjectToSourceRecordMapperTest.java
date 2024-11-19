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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.function.IOSupplier;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3ObjectToSourceRecordMapperTest {

    private Transformer transformer;

    private S3SourceConfig s3SourceConfig;

    private TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;

    private OffsetManager offsetManager;

    private S3ObjectToSourceRecordMapper underTest;

    private S3OffsetManagerEntry offsetManagerEntry;

    private S3Object s3Object = mock(S3Object.class);

    @BeforeEach
    public void setUp() {
        transformer = mock(Transformer.class);
        topicPartitionExtractingPredicate = mock(TopicPartitionExtractingPredicate.class);
        offsetManagerEntry = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "topic", 1);
        when(topicPartitionExtractingPredicate.getOffsetMnagerEntry()).thenReturn(offsetManagerEntry);
        s3SourceConfig = mock(S3SourceConfig.class);
        s3Object = mock(S3Object.class);
        when(s3Object.getKey()).thenReturn("s3ObjectKey");
        offsetManager = mock(OffsetManager.class);
    }

    @Test
    void noS3ObjectContentContentTest() {
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
        final Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result.hasNext()).isFalse();
    }

    void assertS3RecordMatches(final S3SourceRecord s3SourceRecord, final String key, final String value) {
        assertThat(s3SourceRecord.getRecordKey()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
        assertThat(s3SourceRecord.getRecordValue()).isEqualTo(value.getBytes(StandardCharsets.UTF_8));
    }

    private S3ObjectToSourceRecordMapper singleRecordMapper(final Transformer transformer) {
        final S3ObjectInputStream inputStream = new S3ObjectInputStream(
                new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
    }

    private S3ObjectToSourceRecordMapper doubleRecordMapper() {
        final S3ObjectInputStream inputStream = new S3ObjectInputStream(
                new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(new TestingTransformer() {
            @Override
            public Stream<Object> getRecords(final IOSupplier<InputStream> inputStream, final String topic,
                                             final int topicPartition, final S3SourceConfig s3SourceConfig) {
                final Stream<Object> streamResult = super.getRecords(inputStream, topic, topicPartition,
                        s3SourceConfig);
                final List<Object> res = streamResult.collect(Collectors.toList());
                res.add("Transformed2");
                return res.stream();
            }
        }, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
    }

    @Test
    void singleRecordTest() {
        underTest = singleRecordMapper(new TestingTransformer());
        final Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        final S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void multipleRecordTest() {
        underTest = doubleRecordMapper();
        final Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result).hasNext();
        s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed2");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(2L);
    }

    @Test
    void transformerRuntimeExceptionTest() {
        underTest = singleRecordMapper(new TestingTransformer() {
            @Override
            public Stream<Object> getRecords(final IOSupplier<InputStream> inputStream, final String topic,
                                             final int topicPartition, final S3SourceConfig s3SourceConfig) {
                throw new ConnectException("BOOM!");
            }
        });
        final Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result.hasNext()).isFalse();
    }

    @Test
    void transformerIOExceptionTest() {
        final InputStream baseInputStream = new ByteArrayInputStream( // NOPMD
                "Original value".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() throws IOException {
                throw new IOException("BOOM!");
            }
        };
        final S3ObjectInputStream inputStream = new S3ObjectInputStream(baseInputStream, mock(HttpRequestBase.class)); // NOPMD
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
        underTest = singleRecordMapper(new TestingTransformer());
        final Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertThat(result).hasNext();
        final S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertThat(offsetManagerEntry.getRecordCount()).isEqualTo(1L);
        assertThat(result.hasNext()).isFalse();
    }
}