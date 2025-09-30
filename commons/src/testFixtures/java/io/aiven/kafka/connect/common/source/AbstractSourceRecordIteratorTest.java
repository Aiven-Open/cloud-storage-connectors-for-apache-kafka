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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.format.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.format.JsonTestDataFixture;
import io.aiven.kafka.connect.common.format.ParquetTestDataFixture;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A testing fixture that tests an {@link AbstractSourceRecordIterator} implementation.
 *
 * @param <N>
 *            The Native object type.
 * @param <K>
 *            The native key type.
 * @param <O>
 *            The OffsetManagerEntry type.
 * @param <T>
 *            The concrete implementation of the {@link AbstractSourceRecord} .
 */
public abstract class AbstractSourceRecordIteratorTest<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>> {
    /** The offset manager */
    private OffsetManager<O> offsetManager;
    /** The key based on the file name */
    private K key;
    /** The file name for testing */
    private final static String FILE_NAME = "topic-00001-1741965423180.txt";
    /** The file pattern for the file name */
    public final static String FILE_PATTERN = "{{topic}}-{{partition}}-{{start_offset}}";

    // The abstract methods that must be implemented

    /**
     * Convert a string into the key value for the native object. In most cases the underlying system uses a string so
     * returning the {@code key} argument is appropriate. However, this method provides an opportunity to convert the
     * key into something that the native system would produce.
     *
     * @param key
     *            the key value as a string.
     * @return the native key equivalent of the {@code key} parameter.
     */
    abstract protected K createKFrom(final String key);

    /**
     * Create the instance of the source record iterator to be tested.
     *
     * @param mockConfig
     *            A mock configuration returned by {@link #createMockedConfig()} with additional values added.
     * @param offsetManager
     *            A mock offset manager.
     * @param transformer
     *            The trnasformer to use for the test.
     * @return A configured AbstractSourceRecordIterator.
     */
    abstract protected AbstractSourceRecordIterator<K, N, O, T> createSourceRecordIterator(
            final SourceCommonConfig mockConfig, final OffsetManager<O> offsetManager, final Transformer transformer);

    /**
     * Create a client mutator that will add testing data to the iterator under test.
     *
     * @return A client mutator that will add testing data to the iterator under test.
     */
    abstract protected ClientMutator<N, K, ?> createClientMutator();

    /**
     * Create a mock instance of SourceCommonConfig that is appropriate for the iterator under test.
     *
     * @return A mock instance of SourceCommonConfig that is appropriate for the iterator under test.
     */
    abstract protected SourceCommonConfig createMockedConfig();

    // concrete methods.

    @BeforeEach
    public void setUp() {
        SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(offsetStorageReader.offset(anyMap())).thenReturn(Collections.emptyMap());
        when(offsetStorageReader.offsets(anyCollection())).thenReturn(Collections.emptyMap());
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        offsetManager = new OffsetManager<>(sourceTaskContext);
        key = createKFrom(FILE_NAME);
    }

    /**
     * Create a mock SourceCOnfig with our necessary items added.
     *
     * @param filePattern
     *            The file pattern to match.
     * @param taskId
     *            the task ID for the config.
     * @param maxTasks
     *            The maximum tasks for the config.
     * @param targetTopic
     *            the topic for the config.
     * @return A mock SourceCommonConfig that contains the necessary data for the iterator under test.
     */
    protected SourceCommonConfig mockSourceConfig(final String filePattern, final int taskId, final int maxTasks,
            final String targetTopic) {
        SourceCommonConfig mockConfig = createMockedConfig();
        when(mockConfig.getDistributionType()).thenReturn(DistributionType.OBJECT_HASH);
        when(mockConfig.getTaskId()).thenReturn(taskId);
        when(mockConfig.getMaxTasks()).thenReturn(maxTasks);
        when(mockConfig.getTargetTopic()).thenReturn(targetTopic);
        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
        when(mockConfig.getSourceName()).thenReturn(filePattern);
        return mockConfig;
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("inputFormatList")
    void testEmptyClientReturnsEmptyIterator(final InputFormat format, final byte[] ignore) {
        Transformer transformer = TransformerFactory.getTransformer(format);

        SourceCommonConfig mockConfig = mockSourceConfig(FILE_PATTERN, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(format);

        // verify empty is empty.
        createClientMutator().build();
        AbstractSourceRecordIterator<K, N, O, T> iterator = createSourceRecordIterator(mockConfig, offsetManager,
                transformer);
        assertThat(iterator).isExhausted();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("inputFormatList")
    void testOneObjectReturnsOneObject(final InputFormat format, final byte[] data) {
        Transformer transformer = TransformerFactory.getTransformer(format);
        SourceCommonConfig mockConfig = mockSourceConfig(FILE_PATTERN, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(format);

        // verify one data has one data
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(data)).endOfBlock().build();
        AbstractSourceRecordIterator<K, N, O, T> iterator = createSourceRecordIterator(mockConfig, offsetManager,
                transformer);
        assertThat(iterator).hasNext();
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testThrowsExceptionWhenNextOnEmptyIterator() {
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        SourceCommonConfig mockConfig = mockSourceConfig(FILE_PATTERN, 0, 1, null);
        when(mockConfig.getInputFormat()).thenReturn(InputFormat.BYTES);

        // verify empty is empty.
        createClientMutator().build();
        AbstractSourceRecordIterator<K, N, O, T> iterator = createSourceRecordIterator(mockConfig, offsetManager,
                transformer);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    /**
     * Generates the data for the parameterized tests. Creates iterator for each of the {@link InputFormat} types.
     *
     * @return the data for the parameterized tests.
     * @throws IOException
     *             on data creation error.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    static List<Arguments> inputFormatList() throws IOException {
        List<Arguments> result = new ArrayList<>();
        byte[] bytes;
        for (InputFormat format : InputFormat.values()) {
            switch (format) {
                case BYTES :
                    bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
                    break;
                case AVRO :
                    bytes = AvroTestDataFixture.generateAvroData(1);
                    break;
                case JSONL :
                    bytes = JsonTestDataFixture.generateJsonRecs(1).getBytes(StandardCharsets.UTF_8);
                    break;
                case PARQUET :
                    bytes = ParquetTestDataFixture.generateParquetData("name", 1);
                    break;
                default :
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }
            result.add(Arguments.of(format, bytes));
        }
        return result;
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("multiInputFormatList")
    void testMultipleRecordsReturned(final InputFormat format, final byte[] data) {
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(data)).endOfBlock().build();
        Transformer transformer = TransformerFactory.getTransformer(format);
        final SourceCommonConfig config = mockSourceConfig(FILE_PATTERN, 0, 1, null);
        when(config.getTransformerMaxBufferSize()).thenReturn(4096);
        when(config.getInputFormat()).thenReturn(format);
        AbstractSourceRecordIterator<K, N, O, T> iterator = createSourceRecordIterator(config, offsetManager,
                transformer);

        // check first entry
        assertThat(iterator.hasNext()).isTrue();
        T t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(1);

        // check 2nd entry
        assertThat(iterator.hasNext()).isTrue();
        t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(2);

        // check complete
        assertThat(iterator).isExhausted();
    }

    /**
     * Generates the data for the parameterized tests requiring more than one record. Creates iterator for each of the
     * {@link InputFormat} types.
     *
     * @return the data for the parameterized tests.
     * @throws IOException
     *             on data creation error.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    static List<Arguments> multiInputFormatList() throws IOException {
        List<Arguments> result = new ArrayList<>();
        byte[] bytes;
        for (InputFormat format : InputFormat.values()) {
            switch (format) {
                case BYTES :
                    bytes = new byte[4096 * 2];
                    Arrays.fill(bytes, (byte) 5);
                    break;
                case AVRO :
                    bytes = AvroTestDataFixture.generateAvroData(2);
                    break;
                case JSONL :
                    bytes = JsonTestDataFixture.generateJsonRecs(2).getBytes(StandardCharsets.UTF_8);
                    break;
                case PARQUET :
                    bytes = ParquetTestDataFixture.generateParquetData("name", 2);
                    break;
                default :
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }
            result.add(Arguments.of(format, bytes));
        }
        return result;
    }

    /**
     * This test sends 6000 bytes to a ByteArrayTransformer that only returns 4096 byte blocks, so this test should
     * return 2 results.
     */
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    @Test
    void testIteratorProcessesMultipleObjectsFromByteArrayTransformer() {
        final int byteArraySize = 6000;
        final byte[] testData = new byte[byteArraySize];
        for (int i = 0; i < byteArraySize; i++) {
            testData[i] = ((Integer) i).byteValue();
        }
        createClientMutator().reset().addObject(key, ByteBuffer.wrap(testData)).endOfBlock().build();

        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final SourceCommonConfig config = mockSourceConfig(FILE_PATTERN, 0, 1, null);
        when(config.getTransformerMaxBufferSize()).thenReturn(4096);
        when(config.getInputFormat()).thenReturn(InputFormat.BYTES);
        AbstractSourceRecordIterator<K, N, O, T> iterator = createSourceRecordIterator(config, offsetManager,
                transformer);

        // check first entry
        assertThat(iterator.hasNext()).isTrue();
        T t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(1);
        byte[] value = (byte[]) t.getValue().value();
        assertThat(value).as("Initial block match the first 4096 bytes").isEqualTo(Arrays.copyOf(testData, 4096));

        // check 2nd entry
        assertThat(iterator.hasNext()).isTrue();
        t = iterator.next();
        assertThat(t.getRecordCount()).isEqualTo(2);
        value = (byte[]) t.getValue().value();
        assertThat(value).as("Second block should match the remaining bytes")
                .isEqualTo(Arrays.copyOfRange(testData, 4096, 6000));

        // check complete
        assertThat(iterator).isExhausted();
    }

    /**
     * A mutator of the mocked client used by the iterator under test.
     * <p>
     * Most client implementations return a list of objects that are available, often with paging. They also are able to
     * detect new data stored on the system while the iterator is running. This framework allows us to test the
     * interaction of the iterator with the client.
     * </p>
     * <p>
     * The data is stored in blocks. A block is the data returned from a single query to the client. A block comprises
     * zero or more native objects. Testing code adds native objects to the mutator. When {@link #build} or
     * {@link #endOfBlock} is called the current objects and associated data are added to the block queue.
     * </p>
     * <p>
     * A standard usage pattern for the ClientMutator is:
     * </p>
     * <ul>
     * <li>create a Mutator</li>
     * <li>add 3 objects</li>
     * <li>mark end of block</li>
     * <li>mark end of block again</li>
     * <li>add 2 object</li>
     * <li>call {@link #build}</li>
     * </ul>
     * <p>
     * this will result in an iterator that does the following:
     * </p>
     * <ul>
     * <li>returns {@code true} to {@code hasNext}</li>
     * <li>returns the 3 objects via the {@code next} call before returning {@code false} to {@code hasNext}.</li>
     * <li>returns {@code false} to {@code hasNext} again</li>
     * <li>returns {@code true} to {@code hasNext}</li>
     * <li>returns the 2 objects via the {@code next} call before returning {@code false} to {@code hasNext}.</li>
     * <li>returns {@code false} to {@code hasNext} thereafter</li>
     * </ul>
     * </p>
     * <p>
     * For an example see the SourceRecordIteratorTest in the s3-source-connector.
     * </p>
     *
     * @param <N>
     *            The native object type the native object type.
     * @param <K>
     *            the key the native key.
     * @param <T>
     *            the concrete Mutator class.
     *
     */
    abstract public static class ClientMutator<N, K extends Comparable<K>, T extends ClientMutator<N, K, T>> {
        /**
         * A queue of native objects and associated data.
         */
        protected Queue<Pair<List<N>, Map<K, ByteBuffer>>> blocks = new LinkedList<>();
        /**
         * The a list of native objects found in a single block.
         */
        protected List<N> objects = new ArrayList<>();
        /**
         * The map of object keys to data.
         */
        private Map<K, ByteBuffer> data = new HashMap<>();

        /**
         * Create an object of type N. May be a mock object.
         *
         * @param key
         *            the Key for the object.
         * @param data
         *            the data to associate with the object.
         * @return An object of type N.
         */
        abstract protected N createObject(final K key, final ByteBuffer data);

        /**
         * Extracts the blocks from the mutator and creates a client that will return the blocks in order on calls to
         * the methods to get the available record information. The client should be implemented in the concrete test
         * class and need not be exposed here.
         */
        abstract public void build();

        /**
         * Gets the data for the specified key from the data map.
         *
         * @param key
         *            the key to retrieve.
         * @return the data associated with the key or {@code null}.
         */
        final protected ByteBuffer getData(final K key) {
            return data.get(key);
        }

        /**
         * Dequeue a block of data.
         */
        final protected void dequeueBlock() {
            if (blocks.isEmpty()) {
                reset();
            } else {
                final Pair<List<N>, Map<K, ByteBuffer>> pair = blocks.remove();
                objects = pair.getLeft();
                data = pair.getRight();
            }
        }

        /**
         * Adds an object to the block.
         *
         * @param key
         *            the key for the native object.
         * @param data
         *            the data for the native object. String is converted to bytes using UTF-8 encoding.
         * @return this.
         */
        final public T addObject(final K key, final String data) {
            return addObject(key, ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
        }

        /**
         * Adds an object to the block.
         *
         * @param key
         *            the key for the native object.
         * @param data
         *            the data for the native object.
         * @return this.
         */
        final public T addObject(final K key, final ByteBuffer data) {
            objects.add(createObject(key, data));
            this.data.put(key, data);
            return (T) this;
        }

        /**
         * Mark the end of a block and the start of a new one.
         *
         * @return this.
         */
        final public T endOfBlock() {
            blocks.add(Pair.of(objects, data));
            return reset();
        }

        /**
         * reset the objects and data to their empty state. Does not remove already generated blocks.
         *
         * @return this.
         */
        final public T reset() {
            objects = new ArrayList<>();
            data = new HashMap<>();
            return (T) this;
        }
    }
}
