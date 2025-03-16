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

package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.common.source.input.ParquetTestDataFixture;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Stream;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.ExcessiveImports")
public abstract class AbstractSourceRecordIteratorTest<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<N, K, O, T>> {


    private OffsetManager<O> mockOffsetManager;
    private K key;
    private final String fileName = "topic-00001-1741965423180.txt";
    private final String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";

    abstract protected K createKFrom(String key);
    abstract protected AbstractSourceRecordIterator<N, K, O, T> createSourceRecordIterator(SourceCommonConfig mockConfig, OffsetManager<O> mockOffsetManager, Transformer mockTransformer);
    abstract protected ClientMutator<N, K, ?> createClientMutator();
    abstract protected SourceCommonConfig createMockedConfig(final String filePattern);
    abstract protected ConfigDef getConfigDef();
    abstract protected SourceCommonConfig createConfig(final Map<String, String> data);

    @BeforeEach
    public void setUp() {
        mockOffsetManager = mock(OffsetManager.class);
        key = createKFrom(fileName);
    }


    private SourceCommonConfig mockSourceConfig(final String filePattern, final int taskId, final int maxTasks,final String targetTopic ){
        SourceCommonConfig mockConfig = createMockedConfig(filePattern);
        when(mockConfig.getDistributionType()).thenReturn(DistributionType.OBJECT_HASH);
        when(mockConfig.getTaskId()).thenReturn(taskId);
        when(mockConfig.getMaxTasks()).thenReturn(maxTasks);
        when(mockConfig.getTargetTopic()).thenReturn(targetTopic);
        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
        return mockConfig;
    }

    private SourceCommonConfig realSourceConfig(final String filePattern, final int taskId, final int maxTasks,final String targetTopic ) {
        Map<String, String> data = new HashMap<>();

        data.put(SourceCommonConfig.TASK_ID, Integer.toString(taskId));
        data.put(SourceCommonConfig.MAX_TASKS, Integer.toString(maxTasks));

        SourceConfigFragment.Setter sourceSetter = new SourceConfigFragment.Setter(data);
        sourceSetter.setDistributionType(DistributionType.OBJECT_HASH);
        sourceSetter.setTargetTopic(targetTopic);

        FileNameFragment.Setter fileNameSetter = new FileNameFragment.Setter(data);
        fileNameSetter.setFilenameTemplate(filePattern);

        TransformerFragment.Setter transformerSetter = new TransformerFragment.Setter(data);
        transformerSetter.setTransformerMaxBufferSize(4096);

        return createConfig(data);

    }

    @ParameterizedTest(name="{index} {0}")
    @MethodSource("inputFormatList")
    void testEmptyClientReturnsEmptyIterator(InputFormat format, byte[] ignore) throws Exception {
        Transformer transformer = TransformerFactory.getTransformer(format);

        SourceCommonConfig mockConfig = mockSourceConfig(filePattern, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(format);

        //verify empty is empty.
        createClientMutator().build();
        AbstractSourceRecordIterator<N, K, O, T> iterator = createSourceRecordIterator(mockConfig, mockOffsetManager, transformer);
        assertThat(iterator).isExhausted();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @ParameterizedTest(name="{index} {0}")
    @MethodSource("inputFormatList")
    void testOneObjectReturnsOneObject(InputFormat format, byte[] data) throws Exception {
        Transformer transformer = TransformerFactory.getTransformer(format);
        SourceCommonConfig mockConfig = mockSourceConfig(filePattern, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(format);

        // verify one data has one data
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(data)).endOfBlock().build();
        AbstractSourceRecordIterator<N, K, O, T>  iterator = createSourceRecordIterator(mockConfig, mockOffsetManager, transformer);
        assertThat(iterator).hasNext();
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testThrowsExceptionWhenNextOnEmptyIterator() throws Exception {
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        SourceCommonConfig mockConfig = mockSourceConfig(filePattern, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(InputFormat.BYTES);

        //verify empty is empty.
        createClientMutator().build();
        AbstractSourceRecordIterator<N, K, O, T> iterator = createSourceRecordIterator(mockConfig, mockOffsetManager, transformer);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }


    static List<Arguments> inputFormatList() throws IOException {
        List<Arguments> result = new ArrayList<>();
        byte[] bytes;
        for (InputFormat format : InputFormat.values()) {
            switch (format) {
                case BYTES:
                    bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
                    break;
                case AVRO:
                    bytes = AvroTestDataFixture.generateMockAvroData(1);
                    break;
                case JSONL:
                    bytes = JsonTestDataFixture.getJsonRecs(1).getBytes(StandardCharsets.UTF_8);
                    break;
                case PARQUET:
                    bytes = ParquetTestDataFixture.generateMockParquetData("name", 1);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }
            result.add(Arguments.of(format, bytes));
        }
        return result;
    }


    @ParameterizedTest(name="{index} {0}")
    @MethodSource("multiInputFormatList")
    void testMultipleRecordsReturned(InputFormat format, byte[] data) throws Exception {
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(data)).endOfBlock().build();
        Transformer transformer = TransformerFactory.getTransformer(format);
        final SourceCommonConfig config = mockSourceConfig(filePattern, 0, 1, null);
        when(config.getTransformerMaxBufferSize()).thenReturn(4096);
        when(config.getInputFormat()).thenReturn(format);
        AbstractSourceRecordIterator<N, K, O, T> iterator = createSourceRecordIterator(config, mockOffsetManager, transformer);

        // check first entry
        assertThat(iterator.hasNext()).isTrue();
        T t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(0);

        // check 2nd entry
        assertThat(iterator.hasNext()).isTrue();
        t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(0);

        // check complete
        assertThat(iterator).isExhausted();
    }

    static List<Arguments> multiInputFormatList() throws IOException {
        List<Arguments> result = new ArrayList<>();
        byte[] bytes;
        for (InputFormat format : InputFormat.values()) {
            switch (format) {
                case BYTES:
                    bytes = new byte[4096*2];
                    Arrays.fill(bytes, (byte) 5);
                    break;
                case AVRO:
                    bytes = AvroTestDataFixture.generateMockAvroData(2);
                    break;
                case JSONL:
                    bytes = JsonTestDataFixture.getJsonRecs(2).getBytes(StandardCharsets.UTF_8);
                    break;
                case PARQUET:
                    bytes = ParquetTestDataFixture.generateMockParquetData("name", 2);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }
            result.add(Arguments.of(format, bytes));
        }
        return result;
    }


    /**
     * This test sends 6000 bytes to a ByteArrayTransformer that only returns 4096 byte blocks, so this test
     * should return 2 results.
     * @throws Exception
     */
    @Test
    void testIteratorProcessesMultipleObjectsFromByteArrayTransformer() throws Exception {
        final int byteArraySize = 6000;
        final byte[] testData = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData[i] = ((Integer) i).byteValue();
        }
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(testData)).endOfBlock().build();

        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final SourceCommonConfig config = mockSourceConfig(filePattern, 0, 1, null);
        when(config.getTransformerMaxBufferSize()).thenReturn(4096);
        when(config.getInputFormat()).thenReturn(InputFormat.BYTES);
        AbstractSourceRecordIterator<N, K, O, T> iterator = createSourceRecordIterator(config, mockOffsetManager, transformer);

        // check first entry
        assertThat(iterator.hasNext()).isTrue();
        T t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(0);
        byte[] value = (byte[]) t.getValue().value();
        assertThat(value).isEqualTo(Arrays.copyOf(testData, 4096));

        // check 2nd entry
        assertThat(iterator.hasNext()).isTrue();
        t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(0);
        value = (byte[]) t.getValue().value();
        assertThat(value).isEqualTo(Arrays.copyOfRange(testData, 4096, 6000));

        // check complete
        assertThat(iterator).isExhausted();
    }



//        @ParameterizedTest
//        @CsvSource({ "4, 0", "4, 1", "4, 2", "4, 3" })
//        void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId) {
//
//            Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//            final SourceCommonConfig config = mockSourceConfig(filePattern, 0, 1, null);
//            when(config.getTransformerMaxBufferSize()).thenReturn(4096);
//            when(config.getInputFormat()).thenReturn(InputFormat.BYTES);
//            AbstractSourceRecordIterator<N, K, O, T> iterator = createSourceRecordIterator(config, mockOffsetManager, transformer);
//
//
//
//
//            final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, mockOffsetManager, mockTransformer,
//                    sourceApiClient);
//
//            final Predicate<S3Object> s3ObjectPredicate = s3Object -> iterator.taskAssignment
//                    .test(iterator.fileMatching.apply(s3Object));
//            assertThat(s3ObjectPredicate).accepts(obj);
//
//        }
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


    /**
     *
     * @param <N> The native object type
     * @param <K> the key
     * @param <T> the concrete Mutator class.
     */
    abstract public static class ClientMutator<N, K extends Comparable<K>, T extends ClientMutator<N,K,T>> {
        protected Queue<Pair<List<N>, Map<K, ByteBuffer>>> blocks = new LinkedList<>();
        protected List<N> objects = new ArrayList<>();
        private Map<K, ByteBuffer> data = new HashMap<>();
        protected T self;

        protected ClientMutator() {
            self = (T) this;
        }

        abstract protected N createObject(final K key, final ByteBuffer data);

        protected ByteBuffer getData(final K key) {
            return data.get(key);
        }


        protected void dequeueBlock() {
            if (blocks.isEmpty()) {
                reset();
            } else {
                final Pair<List<N>, Map<K, ByteBuffer>> pair = blocks.remove();
                objects = pair.getLeft();
                data = pair.getRight();
            }
        }
        final public T addObject(final K key, final String data) {
            return addObject(key, ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
        }

        final public T addObject(final K key, final ByteBuffer data) {
            objects.add(createObject(key, data));
            this.data.put(key, data);
            return self;
        }

        final public T endOfBlock() {
            blocks.add(Pair.of(objects, data));
            return reset();
        }

        final public T reset() {
            objects = new ArrayList<>();
            data = new HashMap<>();
            return self;
        }

//        private ResponseBytes getResponse(final String key) {
//            return ResponseBytes.fromByteArray(new byte[0], data.get(key));
//        }
//
//        private ListObjectsV2Response dequeueData() {
//            if (blocks.isEmpty()) {
//                objects = Collections.emptyList();
//                data = Collections.emptyMap();
//            } else {
//                final Pair<List<S3Object>, Map<String, byte[]>> pair = blocks.remove();
//                objects = pair.getLeft();
//                data = pair.getRight();
//            }
//            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
//        }

        abstract public void build();
        //{
//            if (!objects.isEmpty()) {
//                endOfBlock();
//            }
//            final S3Client result = mock(S3Client.class);
//            when(result.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
//            when(result.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
//            when(result.getObjectAsBytes(any(GetObjectRequest.class)))
//                    .thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
//            return result;
      //  }
    }
}
