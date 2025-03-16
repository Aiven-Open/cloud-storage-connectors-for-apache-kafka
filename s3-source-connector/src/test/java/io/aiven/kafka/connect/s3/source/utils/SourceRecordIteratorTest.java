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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
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
import java.util.function.Predicate;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
@SuppressWarnings("PMD.ExcessiveImports")
final class SourceRecordIteratorTest extends AbstractSourceRecordIteratorTest<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> {
    private AWSV2SourceClient sourceApiClient;

    private S3ClientBuilder s3ClientBuilder;
    private S3Client s3Client;

    @Override
    protected String createKFrom(String key) {
        return key;
    }

    @Override
    protected AbstractSourceRecordIterator<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> createSourceRecordIterator(SourceCommonConfig mockConfig, OffsetManager<S3OffsetManagerEntry> mockOffsetManager, Transformer mockTransformer) {
        return new SourceRecordIterator((S3SourceConfig) mockConfig, mockOffsetManager,  mockTransformer, new AWSV2SourceClient(s3Client, (S3SourceConfig) mockConfig));
    }

    @Override
    protected ClientMutator<S3Object, String, S3ClientBuilder> createClientMutator() {
        s3ClientBuilder = new S3ClientBuilder();
        return s3ClientBuilder;
    }

    @Override
    protected SourceCommonConfig createMockedConfig(String filePattern) {
        FileNameFragment mockFileNameFrag = mock(FileNameFragment.class);
        S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(mockFileNameFrag.getFilenameTemplate()).thenReturn(Template.of(filePattern));
        when(s3SourceConfig.getFilenameTemplate()).thenReturn(Template.of(filePattern));
        when(s3SourceConfig.getS3FetchBufferSize()).thenReturn(1);
        when(s3SourceConfig.getAwsS3BucketName()).thenReturn("testBucket");
        when(s3SourceConfig.getFetchPageSize()).thenReturn(10);
        return s3SourceConfig;
    }


    @Override
    protected ConfigDef getConfigDef() {
        return S3SourceConfig.configDef();
    }

    @Override
    protected SourceCommonConfig createConfig(Map<String, String> data) {
        data.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
        return new S3SourceConfig(data);
    }

//    private S3SourceConfig mockConfig;
//    private OffsetManager mockOffsetManager;
//    private Transformer mockTransformer;
//    private FileNameFragment mockFileNameFrag;
//
//    private AWSV2SourceClient sourceApiClient;
//
//    @BeforeEach
//    public void setUp() {
//        mockConfig = mock(S3SourceConfig.class);
//        mockOffsetManager = mock(OffsetManager.class);
//        mockTransformer = mock(Transformer.class);
//        mockFileNameFrag = mock(FileNameFragment.class);
//    }
//
//    private S3SourceConfig getConfig(final Map<String, String> data) {
//        final Map<String, String> defaults = new HashMap<>();
//        defaults.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
//        defaults.putAll(data);
//        return new S3SourceConfig(defaults);
//    }
//
//    private void mockSourceConfig(final S3SourceConfig s3SourceConfig, final String filePattern, final int taskId, final int maxTasks,final String targetTopic ){
//        when(s3SourceConfig.getDistributionType()).thenReturn(DistributionType.OBJECT_HASH);
//        when(s3SourceConfig.getTaskId()).thenReturn(taskId);
//        when(s3SourceConfig.getMaxTasks()).thenReturn(maxTasks);
//        when(s3SourceConfig.getS3FileNameFragment()).thenReturn(mockFileNameFrag);
//        when(mockFileNameFrag.getFilenameTemplate()).thenReturn(Template.of(filePattern));
//        when(mockConfig.getTargetTopic()).thenReturn(targetTopic);
//        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
//        when(mockConfig.getS3FetchBufferSize()).thenReturn(1);
//    }
//
//    @Test
//    void testIteratorProcessesS3Objects() throws Exception {
//
//        final String key = "topic-00001-abc123.txt";
//        final String filePattern = "{{topic}}-{{partition}}";
//        final S3SourceConfig config = getConfig(Collections.emptyMap());
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//
//        mockSourceConfig(mockConfig, filePattern, 0, 1, null);
//        when(mockConfig.getInputFormat()).thenReturn(InputFormat.BYTES);
//
//        final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                mockTransformer, sourceApiClient);
//        assertThat(iterator).isExhausted();
//
//        builder.reset().addObject(key, "Hello World").endOfBlock();
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//        final Iterator<S3SourceRecord> s3ObjectIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                mockTransformer, sourceApiClient);
//
//        assertThat(s3ObjectIterator).hasNext();
//        assertThat(s3ObjectIterator.next()).isNotNull();
//        assertThat(s3ObjectIterator).isExhausted();
//
//    }
//
//    @Test
//    void testIteratorExpectExceptionWhenGetsContextWithNoTopic() throws Exception {
//
//        final String key = "topic-00001-abc123.txt";
//        final String filePattern = "{{partition}}";
//        final S3SourceConfig config = getConfig(Collections.emptyMap());
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
//
//        mockSourceConfig(mockConfig, filePattern, 0, 1, null);
//
//        final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                mockTransformer, sourceApiClient);
//        assertThat(iterator).isExhausted();
//
//        builder.reset().addObject(key, "Hello World").endOfBlock();
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//        final Iterator<S3SourceRecord> s3ObjectIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                mockTransformer, sourceApiClient);
//
//        assertThatThrownBy(s3ObjectIterator::next).isInstanceOf(NoSuchElementException.class);
//    }
//
//    @Test
//    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {
//        final String key = "topic-00001-abc123.txt";
//        final String filePattern = "{{topic}}-{{partition}}";
//
//        final S3SourceConfig config = getConfig(Collections.emptyMap());
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        final int byteArraySize = 6000;
//        final byte[] testData = new byte[byteArraySize];
//        for (int i = 0; i < byteArraySize; i++) {
//            testData[i] = ((Integer) i).byteValue();
//        }
//
//        builder.reset().addObject(key, testData).endOfBlock();
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
//
//        mockSourceConfig(mockConfig, filePattern, 0, 1, null);
//        when(mockConfig.getInputFormat()).thenReturn(InputFormat.BYTES);
//        // With ByteArrayTransformer
//        final Iterator<S3SourceRecord> byteArrayIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                transformer, sourceApiClient);
//
//        assertThat(byteArrayIterator.hasNext()).isTrue();
//
//        // Expect 2 items as the transformer will use the default 4096 bytes to split the testdata into two chunks.
//        assertThat(byteArrayIterator.next()).isNotNull();
//        assertThat(byteArrayIterator.next()).isNotNull();
//
//        assertThat(byteArrayIterator).isExhausted();
//
//        // With AvroTransformer all items are already exhausted so nothing should be left.
//        transformer = TransformerFactory.getTransformer(InputFormat.AVRO);
//
//        final Iterator<S3SourceRecord> avroIterator = new SourceRecordIterator(mockConfig, mockOffsetManager,
//                transformer, sourceApiClient);
//        assertThat(avroIterator).isExhausted();
//    }
//
////    @ParameterizedTest
////    @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
////    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
////            final String objectKey) {
////
////        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
////        final String filePattern = "{{partition}}";
////        final String topic = "topic";
////        mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
////        final S3Object obj = S3Object.builder().key(objectKey).build();
////        sourceApiClient = mock(AWSV2SourceClient.class);
////
////        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
////                sourceApiClient);
////
////        final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.taskAssignment
////                .test(iterator.fileMatching.apply(s3Object));
////        assertThat(s3ObjectPredicate).accepts(obj);
////
////    }
//
////    @ParameterizedTest
////    @CsvSource({ "4, 1, topic1-2-0", "4, 3,key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1,key3",
////            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
////    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
////            final String objectKey) {
////        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
////        final String filePattern = "{{partition}}";
////        final String topic = "topic";
////        mockSourceConfig(mockConfig, filePattern, taskId, maxTasks, topic);
////        final S3Object obj = S3Object.builder().key(objectKey).build();
////        sourceApiClient = mock(AWSV2SourceClient.class);
////
////        final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
////                sourceApiClient);
////        final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.taskAssignment
////                .test(iterator.fileMatching.apply(s3Object));
////        // Assert
////        assertThat(s3ObjectPredicate.test(obj)).as("Predicate should accept the objectKey: " + objectKey).isFalse();
////    }
//
//    @Test
//    void testS3ClientIteratorMock() {
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        builder.addObject("Key", "value");
//        final S3Client client = builder.build(); // NOPMD is asking to close client is done so on line 254
//        final ListObjectsV2Response response = client.listObjectsV2(ListObjectsV2Request.builder().build());
//        client.close();
//        assertThat(response.contents()).isNotEmpty();
//
//        sourceApiClient = new AWSV2SourceClient(builder.build(), getConfig(Collections.emptyMap()));
//        final Iterator<S3Object> iterator = sourceApiClient.getS3ObjectStream(null).iterator();
//        assertThat(iterator.hasNext()).isTrue();
//
//    }
//
    class S3ClientBuilder extends ClientMutator<S3Object, String, S3ClientBuilder> {

        protected S3Object createObject(final String key, final ByteBuffer data) {
            return S3Object.builder().key(key).size((long) data.capacity()).build();
        }

        private ResponseBytes getResponse(final String key) {
            return ResponseBytes.fromByteArray(new byte[0], getData(key).array());
        }

        private ListObjectsV2Response dequeueData() {
            dequeueBlock();
            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
        }

        public void build() {
            if (!objects.isEmpty()) {
                endOfBlock();
            }
            s3Client = mock(S3Client.class);
            when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
            when(s3Client.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
            when(s3Client.getObjectAsBytes(any(GetObjectRequest.class)))
                    .thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
        }
    }

}

