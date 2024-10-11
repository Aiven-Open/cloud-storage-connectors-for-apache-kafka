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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.AivenKafkaConnectS3SourceConnector;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputFormat;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class FileReaderTest {

    private static final String TEST_BUCKET = "test-bucket";
    @Mock
    private AmazonS3 s3Client;

    @Mock
    private OffsetManager offsetManager;

    private FileReader fileReader;

    private Map<String, String> properties;

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>();
        setBasicProperties();
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(properties);
        offsetManager = mock(OffsetManager.class);
        fileReader = new FileReader(s3SourceConfig, TEST_BUCKET, Collections.emptySet(), offsetManager);
        s3Client = mock(AmazonS3.class);
    }

    @Test
    void testFetchObjectSummariesWithNoObjects() throws IOException {
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(Collections.emptyList(), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);
        when(offsetManager.getOffsets()).thenReturn(new HashMap<>());

        final List<S3ObjectSummary> summaries = fileReader.fetchObjectSummaries(s3Client);
        assertThat(summaries.size()).isEqualTo(0);
    }

    @Test
    void testFetchObjectSummariesWithOneNonZeroByteObject() throws IOException {
        final S3ObjectSummary objectSummary = createObjectSummary(1);
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(
                Collections.singletonList(objectSummary), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);
        when(offsetManager.getOffsets()).thenReturn(new HashMap<>());

        final List<S3ObjectSummary> summaries = fileReader.fetchObjectSummaries(s3Client);

        assertThat(summaries.size()).isEqualTo(1);
        assertThat(summaries.get(0).getSize()).isEqualTo(1);
    }

    @Test
    void testFetchObjectSummariesWithZeroByteObject() throws IOException {
        final S3ObjectSummary zeroByteObject = createObjectSummary(0);
        final S3ObjectSummary nonZeroByteObject = createObjectSummary(1);
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(
                List.of(zeroByteObject, nonZeroByteObject), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);
        when(offsetManager.getOffsets()).thenReturn(new HashMap<>());

        final List<S3ObjectSummary> summaries = fileReader.fetchObjectSummaries(s3Client);

        assertThat(summaries.size()).isEqualTo(1);
        assertThat(summaries.get(0).getSize()).isEqualTo(1);
    }

    @Test
    void testFetchObjectSummariesWithPagination() throws IOException {
        final S3ObjectSummary object1 = createObjectSummary(1);
        final S3ObjectSummary object2 = createObjectSummary(2);
        final List<S3ObjectSummary> firstBatch = List.of(object1);
        final List<S3ObjectSummary> secondBatch = List.of(object2);

        final ListObjectsV2Result firstResult = createListObjectsV2Result(firstBatch, "nextToken");
        final ListObjectsV2Result secondResult = createListObjectsV2Result(secondBatch, null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult).thenReturn(secondResult);
        when(offsetManager.getOffsets()).thenReturn(new HashMap<>());

        final List<S3ObjectSummary> summaries = fileReader.fetchObjectSummaries(s3Client);

        assertThat(summaries.size()).isEqualTo(2);
    }

    private ListObjectsV2Result createListObjectsV2Result(final List<S3ObjectSummary> summaries,
            final String nextToken) {
        final ListObjectsV2Result result = mock(ListObjectsV2Result.class);
        when(result.getObjectSummaries()).thenReturn(summaries);
        when(result.getNextContinuationToken()).thenReturn(nextToken);
        when(result.isTruncated()).thenReturn(nextToken != null);
        return result;
    }

    private S3ObjectSummary createObjectSummary(final long sizeOfObject) {
        final S3ObjectSummary summary = mock(S3ObjectSummary.class);
        when(summary.getSize()).thenReturn(sizeOfObject);
        return summary;
    }

    private void setBasicProperties() {
        properties.put(S3SourceConfig.OUTPUT_FORMAT_KEY, OutputFormat.BYTES.getValue());
        properties.put("name", "test_source_connector");
        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("tasks.max", "1");
        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "testtopic");
    }
}
