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

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.function.Predicate;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.impl.ExampleNativeClient;
import io.aiven.kafka.connect.common.source.impl.ExampleOffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.ExampleSourceRecord;
import io.aiven.kafka.connect.common.source.impl.ExampleSourceRecordIterator;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
class TaskAssignmentTest {

    // private final String fileName = "topic-00001-1741965423180.txt";
    /** The file pattern for the file name */
    // private final String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";

    public static SourceCommonConfig configureMockConfig(final int taskId, final int maxTasks,
            final DistributionType distributionType) {
        final SourceCommonConfig mockConfig = mock(SourceCommonConfig.class);
        when(mockConfig.getDistributionType()).thenReturn(distributionType);
        when(mockConfig.getTaskId()).thenReturn(taskId);
        when(mockConfig.getMaxTasks()).thenReturn(maxTasks);
        when(mockConfig.getTargetTopic()).thenReturn("topic");
        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
        when(mockConfig.getSourceName()).thenReturn("{{topic}}-{{partition}}-{{start_offset}}");
        return mockConfig;
    }

    @ParameterizedTest
    @CsvSource({ "1", "2", "3", "0" })
    void testThatMatchingHashedKeysAreDetected(final int taskId) {
        final int maxTasks = 4;

        final String[] keys = { "topic-00001-1741965423183.txt", "topic-00001-1741965423180.txt",
                "topic-00001-1741965423181.txt", "topic-00001-1741965423182.txt" };

        final OffsetManager<ExampleOffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        final Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.OBJECT_HASH);
        final ExampleNativeClient nativeClient = mock(ExampleNativeClient.class);
        final ExampleSourceRecordIterator iterator = new ExampleSourceRecordIterator(config, offsetManager, transformer,
                nativeClient);

        final Predicate<Optional<ExampleSourceRecord>> pred = iterator.taskAssignment;
        final ExampleSourceRecord record = mock(ExampleSourceRecord.class);
        for (int i = 0; i < maxTasks; i++) {
            final Context<String> context = new Context<>(keys[i]);
            when(record.getContext()).thenReturn(context);
            if (i == taskId) {
                assertThat(pred.test(Optional.of(record))).isTrue();
            } else {
                assertThat(pred.test(Optional.of(record))).isFalse();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "1", "2", "3", "0" })
    void testThatMatchingPartitionKeysAreDetected(final int taskId) {
        final int maxTasks = 4;

        final String[] keys = { "topic-00001-1741965423183.txt", "topic-00002-1741965423183.txt",
                "topic-00003-1741965423183.txt", "topic-00004-1741965423183.txt" };

        final OffsetManager<ExampleOffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        final Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.PARTITION);
        final ExampleNativeClient nativeClient = mock(ExampleNativeClient.class);
        final ExampleSourceRecordIterator iterator = new ExampleSourceRecordIterator(config, offsetManager, transformer,
                nativeClient);

        final Predicate<Optional<ExampleSourceRecord>> pred = iterator.taskAssignment;
        final ExampleSourceRecord record = mock(ExampleSourceRecord.class);
        for (int i = 0; i < maxTasks; i++) {
            final Context<String> context = new Context<>(keys[i]);
            context.setPartition(i);
            when(record.getContext()).thenReturn(context);
            if (i == taskId) {
                assertThat(pred.test(Optional.of(record))).isTrue();
            } else {
                assertThat(pred.test(Optional.of(record))).isFalse();
            }
        }
    }

    @Test
    void testThatNullKeysAreHandled() {
        final int maxTasks = 4;

        final OffsetManager<ExampleOffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        final Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final ExampleNativeClient nativeClient = mock(ExampleNativeClient.class);

        final ExampleSourceRecord record = mock(ExampleSourceRecord.class);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            final SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.OBJECT_HASH);
            final ExampleSourceRecordIterator iterator = new ExampleSourceRecordIterator(config, offsetManager,
                    transformer, nativeClient);
            final Predicate<Optional<ExampleSourceRecord>> pred = iterator.taskAssignment;
            final Context<String> context = new Context<>(null); // NOPMD AvoidInstantiatingObjectsInLoops
            when(record.getContext()).thenReturn(context);
            assertThat(pred.test(Optional.of(record))).isFalse();
        }
    }

    @Test
    void testThatNullObjectsAreHandled() {
        final int maxTasks = 4;

        final OffsetManager<ExampleOffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        final Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        final ExampleNativeClient nativeClient = mock(ExampleNativeClient.class);

        for (int taskId = 0; taskId < maxTasks; taskId++) {
            final SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.PARTITION);
            final ExampleSourceRecordIterator iterator = new ExampleSourceRecordIterator(config, offsetManager,
                    transformer, nativeClient);
            final Predicate<Optional<ExampleSourceRecord>> pred = iterator.taskAssignment;
            assertThat(pred.test(Optional.empty())).isFalse();
        }
    }

}
