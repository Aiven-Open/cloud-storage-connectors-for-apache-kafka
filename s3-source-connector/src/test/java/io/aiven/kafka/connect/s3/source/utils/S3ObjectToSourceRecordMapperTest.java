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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.function.IOSupplier;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class S3ObjectToSourceRecordMapperTest {

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
        when(topicPartitionExtractingPredicate.getOffsetMapEntry()).thenReturn(offsetManagerEntry);
        s3SourceConfig = mock(S3SourceConfig.class);
        s3Object = mock(S3Object.class);
        when(s3Object.getKey()).thenReturn("s3ObjectKey");
        offsetManager = mock(OffsetManager.class);
    }

    @Test
    void noS3ObjectContentContentTest() {
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertFalse(result.hasNext());
    }

    void assertS3RecordMatches(S3SourceRecord s3SourceRecord, String key, String value) {
        assertArrayEquals(key.getBytes(StandardCharsets.UTF_8), s3SourceRecord.getRecordKey(), key);
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), s3SourceRecord.getRecordValue(), value);
    }

    private S3ObjectToSourceRecordMapper singleRecordMapper(final Transformer transformer) {
        S3ObjectInputStream inputStream = new S3ObjectInputStream(
                new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
    }

    private S3ObjectToSourceRecordMapper doubleRecordMapper() {
        S3ObjectInputStream inputStream = new S3ObjectInputStream(
                new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)),
                mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        return new S3ObjectToSourceRecordMapper(new TestingTransformer() {
            @Override
            public Stream<Object> getRecords(IOSupplier<InputStream> inputStream, String topic, int topicPartition,
                    S3SourceConfig s3SourceConfig) {
                Stream<Object> result = super.getRecords(inputStream, topic, topicPartition, s3SourceConfig);
                List<Object> list = result.collect(Collectors.toList());
                list.add("Transformed2");
                return list.stream();
            }
        }, topicPartitionExtractingPredicate, s3SourceConfig, offsetManager);
    }

    @Test
    void singleRecordTest() {
        underTest = singleRecordMapper(new TestingTransformer());
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertTrue(result.hasNext());
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertEquals(1L, offsetManagerEntry.getRecordCount());
        assertFalse(result.hasNext());
    }

    @Test
    void multipleRecordTest() {
        underTest = doubleRecordMapper();
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertTrue(result.hasNext());
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertEquals(1L, offsetManagerEntry.getRecordCount());
        assertTrue(result.hasNext());
        s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed2");
        assertEquals(2L, offsetManagerEntry.getRecordCount());
    }

    @Test
    void transformerRuntimeExceptionTest() {
        underTest = singleRecordMapper(new TestingTransformer() {
            @Override
            public Stream<Object> getRecords(IOSupplier<InputStream> inputStream, String topic, int topicPartition,
                    S3SourceConfig s3SourceConfig) {
                throw new RuntimeException("BOOM!");
            }
        });
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertFalse(result.hasNext());
    }

    @Test
    void transformerIOExceptionTest() {
        InputStream baseInputStream = new ByteArrayInputStream("Original value".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public void close() throws IOException {
                throw new IOException("BOOM!");
            }
        };
        S3ObjectInputStream inputStream = new S3ObjectInputStream(baseInputStream, mock(HttpRequestBase.class));
        when(s3Object.getObjectContent()).thenReturn(inputStream);
        underTest = new S3ObjectToSourceRecordMapper(transformer, topicPartitionExtractingPredicate, s3SourceConfig,
                offsetManager);
        underTest = singleRecordMapper(new TestingTransformer());
        Iterator<S3SourceRecord> result = underTest.apply(s3Object);
        assertTrue(result.hasNext());
        S3SourceRecord s3SourceRecord = result.next();
        assertS3RecordMatches(s3SourceRecord, "s3ObjectKey", "Transformed(Original value)");
        assertEquals(1L, offsetManagerEntry.getRecordCount());
        assertFalse(result.hasNext());
    }
}
