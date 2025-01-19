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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Consumer;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
@SuppressWarnings("PMD.ExcessiveImports")
final class SourceRecordIteratorTest {

    private S3SourceConfig mockConfig;
    private OffsetManager mockOffsetManager;
    private Transformer mockTransformer;
    private FileNameFragment mockFileNameFrag;

    private AWSV2SourceClient sourceApiClient;

    @BeforeEach
    public void setUp() {
        mockConfig = mock(S3SourceConfig.class);
        mockOffsetManager = mock(OffsetManager.class);
        mockTransformer = mock(Transformer.class);
        mockFileNameFrag = mock(FileNameFragment.class);
    }

    private S3SourceConfig getConfig(final Map<String, String> data) {
        final Map<String, String> defaults = new HashMap<>();
        defaults.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
        defaults.putAll(data);
        return new S3SourceConfig(defaults);
    }

    private void mockSourceConfig(final S3SourceConfig s3SourceConfig, final String filePattern, final int taskId, final int maxTasks,final String targetTopic ){
        when(s3SourceConfig.getDistributionType()).thenReturn(DistributionType.OBJECT_HASH);
        when(s3SourceConfig.getTaskId()).thenReturn(taskId);
        when(s3SourceConfig.getMaxTasks()).thenReturn(maxTasks);
        when(s3SourceConfig.getS3FileNameFragment()).thenReturn(mockFileNameFrag);
        when(mockFileNameFrag.getFilenameTemplate()).thenReturn(Template.of(filePattern));
        when(mockConfig.getTargetTopics()).thenReturn(targetTopic);
    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {

        final String key = "topic-00001-abc123.txt";
        final String filePattern = "{{topic}}-{{partition}}";
        final S3SourceConfig config = getConfig(Collections.emptyMap());
        final S3ClientBuilder builder = new S3ClientBuilder();
        sourceApiClient = new AWSV2SourceClient(builder.build(), config);

        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);

        mockSourceConfig(mockConfig, filePattern, 0, 1, null);

        final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                mockTransformer, sourceApiClient);
        assertThat(iterator).isExhausted();

        builder.reset().addObject(key, "Hello World").endOfBlock();
        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
        final Iterator<S3SourceRecord> s3ObjectIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                mockTransformer, sourceApiClient);

        assertThat(s3ObjectIterator).hasNext();
        assertThat(s3ObjectIterator.next()).isNotNull();
        assertThat(s3ObjectIterator).isExhausted();

    }

    @Test
    void testIteratorExpectExceptionWhenGetsContextWithNoTopic() throws Exception {

        final String key = "topic-00001-abc123.txt";
        final String filePattern = "{{partition}}";
        final S3SourceConfig config = getConfig(Collections.emptyMap());
        final S3ClientBuilder builder = new S3ClientBuilder();
        sourceApiClient = new AWSV2SourceClient(builder.build(), config);

        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);

        mockSourceConfig(mockConfig, filePattern, 0, 1, null);

        final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                mockTransformer, sourceApiClient);
        assertThat(iterator).isExhausted();

        builder.reset().addObject(key, "Hello World").endOfBlock();
        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
        final Iterator<S3SourceRecord> s3ObjectIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
                mockTransformer, sourceApiClient);

        assertThatThrownBy(s3ObjectIterator::hasNext).isInstanceOf(NoSuchElementException.class)
                .hasMessage("No value present");

    }

    // @Test
    // void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {
    // final String key = "topic-00001-abc123.txt";
    // final String filePattern = "{{topic}}-{{partition}}";
    //
    // final S3SourceConfig config = getConfig(Collections.emptyMap());
    // final S3ClientBuilder builder = new S3ClientBuilder();
    //
    // builder.reset().addObject(key, "Hello World").endOfBlock();
    // sourceApiClient = new AWSV2SourceClient(builder.build(), config);
    //
    // mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
    //
    //// when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
    //
    // mockSourceConfig(mockConfig, filePattern, 0, 1, null);
    //
    // // With ByteArrayTransformer
    //
    // mockTransformer = mock(ByteArrayTransformer.class);
    // when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
    // .thenReturn(Stream.of(SchemaAndValue.NULL));
    //
    //// when(mockOffsetManager.getOffsets()).thenReturn(Collections.emptyMap());
    ////
    //// when(mockOffsetManager.recordsProcessedForObjectKey(anyMap(), anyString()))
    //// .thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);
    //
    // // should skip if any records were produced by source record iterator.
    // final Iterator<S3SourceRecord> byteArrayIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
    // mockTransformer, sourceApiClient);
    //
    // assertThat(byteArrayIterator).isExhausted();
    //
    // verify(mockTransformer, never()).getRecords(any(), anyString(), anyInt(), any(), anyLong());
    //
    // // With AvroTransformer
    //
    // mockTransformer = mock(AvroTransformer.class);
    //
    //// when(mockOffsetManager.recordsProcessedForObjectKey(anyMap(), anyString()))
    //// .thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);
    //
    // when(mockTransformer.getKeyData(anyString(), anyString(), any())).thenReturn(SchemaAndValue.NULL);
    // when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
    // .thenReturn(Arrays.asList(SchemaAndValue.NULL).stream());
    //
    // final Iterator<S3SourceRecord> avroIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
    // mockTransformer, sourceApiClient);
    // assertThat(avroIterator).isExhausted();
    //
    // verify(mockTransformer, times(0)).getRecords(any(), anyString(), anyInt(), any(), anyLong());
    //
    // }

    // @ParameterizedTest
    // @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
    // void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
    // final String objectKey) {
    //
    // mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
    //
    // final String key = "topic-00001-abc123.txt";
    // final String filePattern = "{{partition}}";
    // final String topic = "topic";
    // final S3SourceConfig config = getConfig(Collections.emptyMap());
    // final S3ClientBuilder builder = new S3ClientBuilder();
    // mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
    //
    // // Build s3 Client
    // builder.reset().addObject(key, "Hello World").endOfBlock();
    // sourceApiClient = new AWSV2SourceClient(builder.build(), config);
    //
    // final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
    // sourceApiClient);
    // assertThat(iterator).hasNext();
    // final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.isFileMatchingPattern(s3Object)
    // && iterator.isFileAssignedToTask(filePatternUtils.process(s3Object.key()).orElseThrow(), taskId);
    // // Assert
    // assertThat(s3ObjectPredicate).accepts(obj);
    // }

    // @ParameterizedTest
    // @CsvSource({ "4, 1, topic1-2-0", "4, 3,key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1,
    // key3",
    // "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
    // void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
    // final String objectKey) {
    // mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
    // final String filePattern = "{{partition}}";
    // final String topic = "topic";
    // mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
    // final S3ClientBuilder builder = new S3ClientBuilder();
    // final S3SourceConfig config = getConfig(Collections.emptyMap());
    //
    // builder.reset().addObject(objectKey, "Hello World").endOfBlock();
    // sourceApiClient = new AWSV2SourceClient(builder.build(), config);
    //
    // final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
    // sourceApiClient);
    // assertThat(iterator).hasNext();
    // final Predicate<S3Object> stringPredicate = s3Object -> iterator.isFileMatchingPattern(s3Object)
    // && iterator.isFileAssignedToTask(filePatternUtils.process(s3Object.key()).orElseThrow(), taskId);
    // // Assert
    // assertThat(stringPredicate.test(obj)).as("Predicate should accept the objectKey: " + objectKey).isFalse();
    // }

    @Test
    void testS3ClientIteratorMock() {
        final S3ClientBuilder builder = new S3ClientBuilder();
        builder.addObject("Key", "value");
        final S3Client client = builder.build(); // NOPMD is asking to close client is done so on line 254
        final ListObjectsV2Response response = client.listObjectsV2(ListObjectsV2Request.builder().build());
        client.close();
        assertThat(response.contents()).isNotEmpty();

        sourceApiClient = new AWSV2SourceClient(builder.build(), getConfig(Collections.emptyMap()));
        final Iterator<S3Object> iterator = sourceApiClient.getS3ObjectIterator(null);
        assertThat(iterator.hasNext()).isTrue();

    }

    static class S3ClientBuilder {
        Queue<Pair<List<S3Object>, Map<String, byte[]>>> blocks = new LinkedList<>();
        List<S3Object> objects = new ArrayList<>();
        Map<String, byte[]> data = new HashMap<>();

        public S3ClientBuilder addObject(final String key, final byte[] data) {
            objects.add(S3Object.builder().key(key).size((long) data.length).build());
            this.data.put(key, data);
            return this;
        }

        public S3ClientBuilder endOfBlock() {
            blocks.add(Pair.of(objects, data));
            return reset();
        }

        public S3ClientBuilder reset() {
            objects = new ArrayList<>();
            data = new HashMap<>();
            return this;
        }

        public S3ClientBuilder addObject(final String key, final String data) {
            return addObject(key, data.getBytes(StandardCharsets.UTF_8));
        }

        private ResponseBytes getResponse(final String key) {
            return ResponseBytes.fromByteArray(new byte[0], data.get(key));
        }

        private ListObjectsV2Response dequeueData() {
            if (blocks.isEmpty()) {
                objects = Collections.emptyList();
                data = Collections.emptyMap();
            } else {
                final Pair<List<S3Object>, Map<String, byte[]>> pair = blocks.remove();
                objects = pair.getLeft();
                data = pair.getRight();
            }
            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
        }

        public S3Client build() {
            if (!objects.isEmpty()) {
                endOfBlock();
            }
            final S3Client result = mock(S3Client.class);
            when(result.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
            when(result.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
            when(result.getObjectAsBytes(any(GetObjectRequest.class)))
                    .thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
            return result;
        }
    }
}
