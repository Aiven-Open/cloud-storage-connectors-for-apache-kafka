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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3ObjectSummaryIteratorTest {

    private static final String BUCKET_NAME = "test-bucket";

    private AmazonS3 s3Client;

    @BeforeEach
    public void initializeSourceClient() {
        s3Client = mock(AmazonS3.class);
    }

    @Test
    void testNoSummaries() {
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(new ListObjectsV2Result());
        final S3ObjectSummaryIterator underTest = new S3ObjectSummaryIterator(s3Client, new ListObjectsV2Request());
        assertThat(underTest).isExhausted();
    }

    @Test
    void testOneSummary() {
        final String objectKey = "any-key";
        initializeSourceClient();
        final S3ObjectSummary objectSummary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, objectKey);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(objectSummary), null);
        S3ObjectsUtils.populateS3Client(s3Client, listObjectsV2Result);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                .thenReturn(new ListObjectsV2Result());

        final S3ObjectSummaryIterator underTest = new S3ObjectSummaryIterator(s3Client, new ListObjectsV2Request());

        assertThat(underTest).hasNext();
        final S3ObjectSummary summary = underTest.next();
        assertThat(summary.getKey()).isEqualTo(objectKey);

        assertThat(underTest).isExhausted();
    }

    @Test
    void testSummariesWithPagination() throws IOException {
        initializeSourceClient();
        final S3ObjectSummary object1 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key1");
        final S3ObjectSummary object2 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key2");
        final List<S3ObjectSummary> firstBatch = List.of(object1);
        final List<S3ObjectSummary> secondBatch = List.of(object2);

        final ListObjectsV2Result firstResult = S3ObjectsUtils.createListObjectsV2Result(firstBatch, "nextToken");
        final ListObjectsV2Result secondResult = S3ObjectsUtils.createListObjectsV2Result(secondBatch, null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult)
                .thenReturn(secondResult)
                .thenReturn(new ListObjectsV2Result());
        S3ObjectsUtils.populateS3Client(s3Client, firstResult);
        S3ObjectsUtils.populateS3Client(s3Client, secondResult);

        final S3ObjectSummaryIterator underTest = new S3ObjectSummaryIterator(s3Client, new ListObjectsV2Request());

        assertThat(underTest).hasNext();
        S3ObjectSummary summary = underTest.next();
        assertThat(summary.getKey()).isEqualTo("key1");

        assertThat(underTest).hasNext();
        summary = underTest.next();
        assertThat(summary.getKey()).isEqualTo("key2");

        assertThat(underTest).isExhausted();
    }
}
