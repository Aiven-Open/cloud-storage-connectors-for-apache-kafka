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
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils.LAST_RESULT;
import static io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils.createObjectSummary;
import static io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils.createListObjectsV2Result;
import static io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils.populateS3Client;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3Object;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.aiven.kafka.connect.s3.source.testutils.S3ObjectsUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

class AWSV2SourceClientTest {

    /**
     * A map of keys to task Id with a max task of 4
     */
    Map<String, Integer> keyTaskMap = Map.of("key1", 2, "key2", 3, "key3", 0, "key4", 1);

    private AmazonS3 s3Client;

    private static final String BUCKET_NAME = "test-bucket";

    private AWSV2SourceClient awsv2SourceClient;

    @Captor
    ArgumentCaptor<ListObjectsV2Request> requestCaptor;

    private static Map<String, String> getConfigMap(final int maxTasks, final int taskId) {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("tasks.max", String.valueOf(maxTasks));
        configMap.put("task.id", String.valueOf(taskId));

        configMap.put(AWS_S3_BUCKET_NAME_CONFIG, BUCKET_NAME);
        return configMap;
    }

    public void initializeSourceClient(final int maxTasks, final int taskId) {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(getConfigMap(maxTasks, taskId));
        s3Client = mock(AmazonS3.class);
        awsv2SourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());
    }

    public void initializeSourceClient() {
        initializeSourceClient(1, 0);
    }

    @ParameterizedTest
    @CsvSource({"3, 1", "1, 0"})
    void testFetchObjectsWithNoObjects(final int maxTasks, final int taskId) {
        initializeSourceClient(maxTasks, taskId);
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(Collections.emptyList(), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result).thenReturn(LAST_RESULT);

        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);
        assertThat(objects).isExhausted();
    }

    @Test
    void testFetchOneObjectWithBasicConfig() {
        final String objectKey = "any-key";
        initializeSourceClient(1, 0);
        ListObjectsV2Result result = createListObjectsV2Result(List.of(S3ObjectsUtils.createObjectSummary(BUCKET_NAME, objectKey)), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result).thenReturn(LAST_RESULT);
        populateS3Client(s3Client, result);


        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);
        assertThat(objects).hasNext();
        S3Object object = objects.next();
        assertThat(object.getKey()).isEqualTo(objectKey);
    }

    @ParameterizedTest
    @CsvSource({"key1", "key2", "key3", "key4"})
    void testFetchObjectsWithWithTaskIdAssigned(final String objectKey) {
        initializeSourceClient(4, keyTaskMap.get(objectKey));
        List<S3ObjectSummary> lst = new ArrayList<>();
        lst.add(S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key1"));
        lst.add(S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key2"));
        lst.add(S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key3"));
        lst.add(S3ObjectsUtils.createObjectSummary(BUCKET_NAME, "key4"));

        ListObjectsV2Result result = createListObjectsV2Result(lst, null);
        populateS3Client(s3Client, result);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result).thenReturn(LAST_RESULT);
        ;

        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);
        assertThat(objects).hasNext();

        S3Object object = objects.next();
        assertThat(object.getKey()).isEqualTo(objectKey);
        assertThat(objects).isExhausted();
    }

    @ParameterizedTest
    @CsvSource({"4, 1, key1", "4, 3, key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1, key3",
            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4"})
    void testFetchObjectsWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
                                                                      final String objectKey) {
        initializeSourceClient(maxTasks, taskId);
        populateS3Client(s3Client, createObjectSummary(BUCKET_NAME, objectKey));
        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);

        assertThat(objects).isExhausted();
    }

    @ParameterizedTest
    @CsvSource({"4, 3", "4, 0"})
    void testFetchObjectsFiltersOutZeroByteObject(final int maxTasks, final int taskId) {
        initializeSourceClient(maxTasks, taskId);
        List<S3ObjectSummary> lst = new ArrayList<>();
        lst.add(createObjectSummary(0, BUCKET_NAME, "key1"));
        lst.add(createObjectSummary(BUCKET_NAME, "key2"));
        lst.add(createObjectSummary(BUCKET_NAME, "key3"));
        final ListObjectsV2Result result = getListObjectsV2Result();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result).thenReturn(LAST_RESULT);
        populateS3Client(s3Client, result);

        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);

        // assigned 1 object to taskid
        assertThat(objects).hasNext();
        objects.next();
        assertThat(objects).isExhausted();
    }

    @Test
    void testFetchObjectWithPrefix() {
        final Map<String, String> configMap = getConfigMap(1, 0);
        configMap.put(AWS_S3_PREFIX_CONFIG, "test/");
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configMap);
        s3Client = mock(AmazonS3.class);
        awsv2SourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());
        requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        final S3ObjectSummary object1 = createObjectSummary(BUCKET_NAME, "key1");
        final S3ObjectSummary object2 = createObjectSummary(BUCKET_NAME, "key2");

        final ListObjectsV2Result firstResult = createListObjectsV2Result(List.of(object1), "nextToken");
        final ListObjectsV2Result secondResult = createListObjectsV2Result(List.of(object2), null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult).thenReturn(secondResult);

        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(null);
        while (objects.hasNext()) {
            objects.next();
        }
        assertThat(objects).isExhausted();

        verify(s3Client, times(2)).listObjectsV2(requestCaptor.capture());
        final List<ListObjectsV2Request> allRequests = requestCaptor.getAllValues();

        assertThat(allRequests.get(0).getPrefix()).isEqualTo(s3SourceConfig.getAwsS3Prefix());
        assertThat(allRequests.get(1).getContinuationToken()).isEqualTo("nextToken");
    }

    @Test
    void testFetchObjectWithInitialStartAfter() {
        final Map<String, String> configMap = getConfigMap(1, 0);
        final String startAfter = "file-option-1-12000.txt";
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configMap);
        s3Client = mock(AmazonS3.class);
        awsv2SourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());
        requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        final S3ObjectSummary object1 = createObjectSummary(BUCKET_NAME, "key1");
        final S3ObjectSummary object2 = createObjectSummary(BUCKET_NAME, "key2");

        final ListObjectsV2Result firstResult = createListObjectsV2Result(List.of(object1), "nextToken");
        final ListObjectsV2Result secondResult = createListObjectsV2Result(List.of(object2), null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult).thenReturn(secondResult);

        final Iterator<S3Object> objects = awsv2SourceClient.getIteratorOfObjects(startAfter);
        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));

        assertThat(objects.next()).isNotNull();
        assertThat(objects.next()).isNotNull();

        verify(s3Client, times(2)).listObjectsV2(requestCaptor.capture());
        final List<ListObjectsV2Request> allRequests = requestCaptor.getAllValues();
        assertThat(objects).isExhausted();

        assertThat(allRequests.get(0).getStartAfter()).isEqualTo(startAfter);
        // Not required with continuation token
        assertThat(allRequests.get(1).getStartAfter()).isNull();
        assertThat(allRequests.get(1).getContinuationToken()).isEqualTo("nextToken");

    }

    private ListObjectsV2Result getListObjectsV2Result() {
        final S3ObjectSummary zeroByteObject = createObjectSummary(0, BUCKET_NAME, "key1");
        final S3ObjectSummary nonZeroByteObject1 = createObjectSummary(BUCKET_NAME, "key2");
        final S3ObjectSummary nonZeroByteObject2 = createObjectSummary(BUCKET_NAME, "key3");
        return createListObjectsV2Result(List.of(zeroByteObject, nonZeroByteObject1, nonZeroByteObject2), null);
    }

    @Test
    void testFetchObjectsWithOneObject() throws IOException {
        final String objectKey = "any-key";
        initializeSourceClient();
        final S3ObjectSummary objectSummary = S3ObjectsUtils.createObjectSummary(BUCKET_NAME, objectKey);
        final ListObjectsV2Result listObjectsV2Result = S3ObjectsUtils
                .createListObjectsV2Result(Collections.singletonList(objectSummary), null);
        S3ObjectsUtils.populateS3Client(s3Client, listObjectsV2Result);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result)
                .thenReturn(new ListObjectsV2Result());

        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getIteratorOfObjects(null);

        assertThat(s3ObjectIterator).hasNext();
        try (S3Object object = s3ObjectIterator.next()) {
            assertThat(object.getKey()).isEqualTo(objectKey);
        }
        assertThat(s3ObjectIterator).isExhausted();
    }

    @Test
    void testFetchObjectsFiltersOutFailedObject() throws IOException {
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
        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getIteratorOfObjects(null);

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
    void testFetchObjectsWithPagination() throws IOException {
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

        final Iterator<S3Object> s3ObjectIterator = awsv2SourceClient.getIteratorOfObjects(null);

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
