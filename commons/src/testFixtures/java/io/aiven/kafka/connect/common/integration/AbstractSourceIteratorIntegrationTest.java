/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.common.integration;

import static io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest.FILE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that the AbstractSourceIterator functions correctly with the underlying storage system.
 *
 * @param <K>
 *            the native key type.
 */
public abstract class AbstractSourceIteratorIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, I extends AbstractSourceRecordIterator<?, K, O, ?>>
        extends
            AbstractIntegrationTest<K, O, I> {

    /**
     * Static value to flag that the task is not set.
     */
    private static final int TASK_NOT_SET = -1;

    @Override
    protected Duration getOffsetFlushInterval() {
        return Duration.ofMillis(500);
    }

    /**
     * Get the Source record iterator under test.
     *
     * @param configData
     *            the configuration data for the iterator.
     * @param offsetManager
     *            the OffsetManager for the iterator to use.d
     * @param transformer
     *            the Transformer for the iterator to use.
     * @return a configured SourceRecord iterator.
     */
    protected abstract I getSourceRecordIterator(Map<String, String> configData, OffsetManager<O> offsetManager,
            Transformer transformer);

    /**
     * Creates an offset manager that does not have any stored Kafka data.
     *
     * @return An offset manager that does not have any stored Kafka data.
     */
    final protected OffsetManager<O> createOffsetManager() {
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());
        return new OffsetManager<>(context);
    }

    /**
     * Creates a configuration for the spcified arguments
     *
     * @param topic
     *            the topic to write to.
     * @param taskId
     *            the taskID, May be {@link #TASK_NOT_SET}.
     * @param maxTasks
     *            the number of tasks.
     * @param inputFormat
     *            the input format to read.
     * @return a map of configuration values that reflect the arguments.
     */
    private Map<String, String> createConfig(final String topic, final int taskId, final int maxTasks,
            final InputFormat inputFormat) {
        return createConfig(null, topic, taskId, maxTasks, inputFormat);
    }

    /**
     * Creates a configuration for the spcified arguments
     *
     * @param localPrefix
     *            the prefix to add to the native key.
     * @param topic
     *            the topic to write to.
     * @param taskId
     *            the taskID, May be {@link #TASK_NOT_SET}.
     * @param maxTasks
     *            the number of tasks.
     * @param inputFormat
     *            the input format to read.
     * @return a map of configuration values that reflect the arguments.
     */
    private Map<String, String> createConfig(final String localPrefix, final String topic, final int taskId,
            final int maxTasks, final InputFormat inputFormat) {
        final Map<String, String> configData = createConnectorConfig(localPrefix);

        KafkaFragment.setter(configData).connector(getConnectorClass());

        SourceConfigFragment.setter(configData).targetTopic(topic);

        final CommonConfigFragment.Setter setter = CommonConfigFragment.setter(configData).maxTasks(maxTasks);
        if (taskId > TASK_NOT_SET) {
            setter.taskId(taskId);
        }

        if (inputFormat == InputFormat.AVRO) {
            configData.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        }
        TransformerFragment.setter(configData).inputFormat(inputFormat);

        FileNameFragment.setter(configData).template(FILE_PATTERN);

        return configData;
    }

    @ParameterizedTest
    @EnumSource(InputFormat.class)
    void zeroLengthFilesAreIgnoredTest(final InputFormat inputFormat) {
        final String topic = getTopic();
        final int maxTasks = 1;
        final int taskId = 0;

        // write empty file object
        write(topic, new byte[0], 3);
        assertThat(getNativeStorage()).hasSize(1);

        final I sourceRecordIterator = getSourceRecordIterator(createConfig(topic, taskId, maxTasks, inputFormat),
                createOffsetManager(), TransformerFactory.getTransformer(inputFormat));

        assertThat(sourceRecordIterator).isExhausted();
    }

    /**
     * Test the integration with the BYTE reader.
     */
    @Test
    void sourceRecordIteratorBytesTest() {
        final String topic = getTopic();
        final int maxTasks = 1;
        final int taskId = 0;

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<K> expectedKeys = new ArrayList<>();
        // write objects to storage
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getNativeKey());

        assertThat(getNativeStorage()).hasSize(4);

        final I sourceRecordIterator = getSourceRecordIterator(createConfig(topic, taskId, maxTasks, InputFormat.BYTES),
                createOffsetManager(), TransformerFactory.getTransformer(InputFormat.BYTES));

        final HashSet<K> seenKeys = new HashSet<>();
        while (sourceRecordIterator.hasNext()) {
            final AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
            final K key = sourceRecord.getNativeKey();
            assertThat(expectedKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(expectedKeys);
    }

    /**
     * Test the integration with the AVRO reader.
     */
    @Test
    void sourceRecordIteratorAvroTest() throws IOException {
        final var topic = getTopic();
        final int maxTasks = 1;
        final int taskId = 0;

        final Map<String, String> configData = createConfig(topic, taskId, maxTasks, InputFormat.AVRO);
        KafkaFragment.setter(configData).valueConverter(AvroConverter.class);

        final int numberOfRecords = 5000;

        final byte[] outputStream1 = AvroTestDataFixture.generateAvroData(1, numberOfRecords);
        final byte[] outputStream2 = AvroTestDataFixture.generateAvroData(numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream3 = AvroTestDataFixture.generateAvroData(2 * numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream4 = AvroTestDataFixture.generateAvroData(3 * numberOfRecords + 1, numberOfRecords);
        final byte[] outputStream5 = AvroTestDataFixture.generateAvroData(4 * numberOfRecords + 1, numberOfRecords);

        final Set<K> offsetKeys = new HashSet<>();

        offsetKeys.add(write(topic, outputStream1, 1).getNativeKey());
        offsetKeys.add(write(topic, outputStream2, 1).getNativeKey());

        offsetKeys.add(write(topic, outputStream3, 2).getNativeKey());
        offsetKeys.add(write(topic, outputStream4, 2).getNativeKey());
        offsetKeys.add(write(topic, outputStream5, 2).getNativeKey());

        assertThat(getNativeStorage()).hasSize(5);

        final I sourceRecordIterator = getSourceRecordIterator(configData, createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.AVRO));

        final HashSet<K> seenKeys = new HashSet<>();
        final Map<K, List<Long>> seenRecords = new HashMap<>();
        while (sourceRecordIterator.hasNext()) {
            final AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
            final K key = sourceRecord.getNativeKey();
            seenRecords.compute(key, (k, v) -> {
                final List<Long> lst = v == null ? new ArrayList<>() : v; // NOPMD new object inside loop
                lst.add(sourceRecord.getOffsetManagerEntry().getRecordCount());
                return lst;
            });
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(offsetKeys);
        assertThat(seenRecords).hasSize(5);
        final List<Long> expected = new ArrayList<>();
        for (long l = 0; l < numberOfRecords; l++) {
            expected.add(l + 1);
        }
        for (final K key : offsetKeys) {
            final List<Long> seen = seenRecords.get(key);
            assertThat(seen).as("Count for " + key).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * Test the iterator can detect rehydration. Rehydration is when additional data items are written to the data store
     * after the iterator has reported exhaustion. The iterator must be able to detect the new records ana process them.
     */
    @Test
    void sourceRecordIteratorRehydrationTest() {
        // create 2 files.
        final var topic = getTopic();
        final Map<String, String> configData = createConfig(topic, 0, 1, InputFormat.BYTES);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final List<K> expectedKeys = new ArrayList<>();

        final List<K> actualKeys = new ArrayList<>();

        // write 2 objects
        expectedKeys.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        expectedKeys.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());

        assertThat(getNativeStorage()).hasSize(2);

        final I sourceRecordIterator = getSourceRecordIterator(configData, createOffsetManager(),
                TransformerFactory.getTransformer(InputFormat.BYTES));
        assertThat(sourceRecordIterator).hasNext();
        AbstractSourceRecord<?, K, O, ?> sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).hasNext();
        sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).isExhausted();
        // ensure checking a 2nd time does not reload old data.
        assertThat(sourceRecordIterator).as("Reloading leads to extra entries").isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);

        // write 3rd object
        expectedKeys.add(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 0).getNativeKey());
        assertThat(getNativeStorage()).hasSize(3);

        assertThat(sourceRecordIterator).hasNext();
        sourceRecord = sourceRecordIterator.next();
        actualKeys.add(sourceRecord.getNativeKey());
        assertThat(sourceRecordIterator).isExhausted();
        assertThat(actualKeys).containsAll(expectedKeys);
    }
}
