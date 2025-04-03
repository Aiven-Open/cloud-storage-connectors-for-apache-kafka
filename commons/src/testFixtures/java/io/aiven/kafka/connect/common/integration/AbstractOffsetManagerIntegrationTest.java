package io.aiven.kafka.connect.common.integration;

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
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest.FILE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public abstract class AbstractOffsetManagerIntegrationTest <K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>,
        I extends AbstractSourceRecordIterator<?, K, O, ?>> extends AbstractIntegrationTest<K, O, I> {

    private static final int TASK_NOT_SET = -1;

    /**
     * Tests offset manager ability to read data from context by:
     * <ol>
     *     <li>writing 4 records</li>
     *     <li>reading them</li>
     *     <li>verifying proper values were read</li>
     *     <li>disabling the ring buffer and restarting the connector</li>
     *     <li>writing one record</li>
     *     <li>reading it</li>
     *     <li>verifying that the proper value was read</li>
     *     </ol>
     *
     *     <p>By disabling the ring buffer the filtering of seen records falls to the OffsetManager exclusively.</p>
     *    <p>If the wrong value is read or no records are read then there was an error in the OffsetManager's attempt
     *    to read and parse the context information.</p>
     */
    @Test
    void OffsetManagerRead() {
        final String topic = getTopic();
        final int partitionId = 0;
        final String prefixPattern = "topics/{{topic}}/partition={{partition}}/";

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 5 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

        final List<OffsetManager.OffsetManagerKey> offsetKeys = new ArrayList<>(expectedOffsetRecords.keySet());
        offsetKeys.add(write(topic, new byte[0], 3).getOffsetManagerKey());

        try {
            // Start the Connector
            final Map<String, String> connectorConfig = createConfig(topic, TASK_NOT_SET, 1, InputFormat.BYTES);
            KafkaFragment.setter(connectorConfig)
                    .name(getConnectorName())
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class);
            SourceConfigFragment.setter(connectorConfig).distributionType(DistributionType.PARTITION);

            KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);

            assertThat(getNativeStorage()).hasSize(5);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

            SourceConfigFragment.setter(connectorConfig)
                            .ringBufferSize(0);

            getLogger().info(">>>>> RESTARTING");
            kafkaManager.configureConnector(getConnectorName(), connectorConfig);
            getLogger().info(">>>>> RESTARTED");

            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(), 1L);

            records = messageConsumer().consumeByteMessages(topic, 1, Duration.ofSeconds(30));

            assertThat(records).containsOnly(testData3);

        } catch (Exception e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }

    private Map<String, String> createConfig(final String topic, final int taskId, final int maxTasks, final InputFormat inputFormat) {
        return createConfig(null, topic, taskId, maxTasks, inputFormat);
    }

    private Map<String, String> createConfig(String localPrefix, final String topic, final int taskId, final int maxTasks, final InputFormat inputFormat) {
        final Map<String, String> configData = createConnectorConfig(localPrefix);

        KafkaFragment.setter(configData)
                .connector(getConnectorClass());

        SourceConfigFragment.setter(configData).targetTopic(topic);

        CommonConfigFragment.Setter setter = CommonConfigFragment.setter(configData).maxTasks(maxTasks);
        if (taskId > TASK_NOT_SET) {
            setter.taskId(taskId);
        }

        if (inputFormat == InputFormat.AVRO)
        {
            configData.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        }
        TransformerFragment.setter(configData).inputFormat(inputFormat);

        FileNameFragment.setter(configData).template(FILE_PATTERN);

        return configData;
    }
}
