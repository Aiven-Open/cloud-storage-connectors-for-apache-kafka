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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AWSV2SourceClientTest {

    private static final String BUCKET_NAME = "test-bucket";

    private AmazonS3 s3Client;

    private AWSV2SourceClient awsv2SourceClient;

    private static Map<String, String> getConfigMap() {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put(AWS_S3_BUCKET_NAME_CONFIG, BUCKET_NAME);
        return configMap;
    }

    @BeforeEach
    public void initializeSourceClient() {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(getConfigMap());
        s3Client = mock(AmazonS3.class);
        awsv2SourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());
    }

    @Test
    void testFetchObjectSummariesWithNoObjects() {
        initializeSourceClient();
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.emptyList(), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);

        final Iterator<S3Object> summaries = awsv2SourceClient.getObjectIterator(null);
        assertThat(summaries).isExhausted();
    }

    @Test
    void testFetchObjectSummariesWithOneObject() throws IOException {
        final String objectKey = "any-key";
        initializeSourceClient();
        final S3ObjectSummary objectSummary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, objectKey);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(objectSummary), null);
        S3ObjectsUtils.populateS3Client(s3Client, listObjectsV2Result);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                .thenReturn(new ListObjectsV2Result());

        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getObjectIterator(null);

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object object = s3ObjectIterator.next()) {
            assertThat(object.getKey()).isEqualTo(objectKey);
        }
        assertThat(s3ObjectIterator).isExhausted();
    }

    @Test
    void testFetchObjectSummariesFiltersOutZeroByteObject() throws IOException {
        initializeSourceClient();

        final S3ObjectSummary zeroByteObject = S3ObjectsUtils.createObjectSummary(0, BUCKET_NAME, "key1");
        final S3ObjectSummary nonZeroByteObject1 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key2");
        final S3ObjectSummary nonZeroByteObject2 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key3");
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(List.of(zeroByteObject, nonZeroByteObject1, nonZeroByteObject2), null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                .thenReturn(new ListObjectsV2Result());
        S3ObjectsUtils.populateS3Client(s3Client, listObjectsV2Result);

        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getObjectIterator(null);

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key2");
        }

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key3");
        }

        assertThat(s3ObjectIterator).isExhausted();
    }

    @Test
    void testFetchObjectSummariesFiltersOutFailedObject() throws IOException {
        initializeSourceClient();

        final S3ObjectSummary zeroByteObject = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key1");
        final S3ObjectSummary nonZeroByteObject1 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key2");
        final S3ObjectSummary nonZeroByteObject2 = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key3");
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(List.of(zeroByteObject, nonZeroByteObject1, nonZeroByteObject2), null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                .thenReturn(new ListObjectsV2Result());
        S3ObjectsUtils.populateS3Client(s3Client, listObjectsV2Result);

        awsv2SourceClient.addFailedObjectKeys("key2");
        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getObjectIterator(null);

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key1");
        }

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key3");
        }

        assertThat(s3ObjectIterator).isExhausted();
    }

    @Test
    void testFetchObjectSummariesWithPagination() throws IOException {
        initializeSourceClient();
        final S3ObjectSummary object1 = S3ObjectsUtils.createObjectSummary(1, BUCKET_NAME, "key1");
        final S3ObjectSummary object2 = S3ObjectsUtils.createObjectSummary(2, BUCKET_NAME, "key2");
        final List<S3ObjectSummary> firstBatch = List.of(object1);
        final List<S3ObjectSummary> secondBatch = List.of(object2);

        final ListObjectsV2Result firstResult = S3ObjectsUtils.createListObjectsV2Result(firstBatch, "nextToken");
        final ListObjectsV2Result secondResult = S3ObjectsUtils.createListObjectsV2Result(secondBatch, null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult)
                .thenReturn(secondResult)
                .thenReturn(new ListObjectsV2Result());
        S3ObjectsUtils.populateS3Client(s3Client, firstResult);
        S3ObjectsUtils.populateS3Client(s3Client, secondResult);

        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getObjectIterator(null);

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key1");
        }

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object s3Object = s3ObjectIterator.next()) {
            assertThat(s3Object.getKey()).isEqualTo("key2");
        }

        assertThat(s3ObjectIterator).isExhausted();
    }

}
