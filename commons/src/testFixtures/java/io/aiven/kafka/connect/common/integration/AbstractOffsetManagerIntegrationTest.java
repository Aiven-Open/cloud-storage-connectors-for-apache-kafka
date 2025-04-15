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
import static org.assertj.core.api.Fail.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.converters.ByteArrayConverter;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.junit.jupiter.api.Test;

/**
 * Test to verify OffsetManager and OffsetManagerKey implementations work as expected
 *
 * @param <K>
 *            the native Key type.
 * @param <O>
 *            The OffsetManagerEntry type.
 * @param <I>
 *            The SourecRecord iteratior type.
 */
public abstract class AbstractOffsetManagerIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, I extends AbstractSourceRecordIterator<?, K, O, ?>>
        extends
        AbstractSourceIntegrationBase<K, O, I> {

    /**
     * Static to indicate that the TASK is not set.
     */
    private static final int TASK_NOT_SET = -1;

    /**
     * Tests offset manager ability to read data from the Kafka context by:
     * <ol>
     * <li>writing 4 records</li>
     * <li>reading them</li>
     * <li>verifying proper values were read</li>
     * <li>disabling the ring buffer and restarting the connector</li>
     * <li>writing one record</li>
     * <li>reading it</li>
     * <li>verifying that the proper value was read</li>
     * </ol>
     *
     * <p>
     * By disabling the ring buffer the filtering of seen records falls to the OffsetManager exclusively.
     * </p>
     * <p>
     * If the wrong value is read or no records are read then there was an error in the OffsetManager's attempt to read
     * and parse the context information.
     * </p>
     */
    @Test
    void offsetManagerRead() {
        final String topic = getTopic();

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 4 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(4);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

            SourceConfigFragment.setter(connectorConfig).ringBufferSize(0);

            getLogger().info(">>>>> RESTARTING");
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);
            getLogger().info(">>>>> RESTARTED");

            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                    1L);

            records = messageConsumer().consumeByteMessages(topic, 1, Duration.ofSeconds(30));

            assertThat(records).containsOnly(testData3);

        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    /**
     * Create the config that does not have a prefix on the native item names.
     *
     * @param topic
     *            the topic to suse
     * @param taskId
     *            the task ID, may be {@link #TASK_NOT_SET}
     * @param maxTasks
     *            the maximum number of tasks.
     * @param inputFormat
     *            the input format for the input.
     * @return a map containing the configuration data that matches the arguments.
     */
    private Map<String, String> createConfig(final String topic, final int taskId, final int maxTasks,
            final InputFormat inputFormat) {
        return createConfig(null, topic, taskId, maxTasks, inputFormat);
    }

    /**
     * Create the config that does have a prefix on the native item names.
     *
     * @param localPrefix
     *            a prefix to prepend to the native item names.
     * @param topic
     *            the topic to suse
     * @param taskId
     *            the task ID, may be {@link #TASK_NOT_SET}
     * @param maxTasks
     *            the maximum number of tasks.
     * @param inputFormat
     *            the input format for the input.
     * @return a map containing the configuration data that matches the arguments.
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
}
