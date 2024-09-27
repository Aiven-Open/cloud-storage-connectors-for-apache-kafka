/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.grouper;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.TestConfig;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.grouper.TestRecordGrouperBuilders.TestRecordGrouper;
import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

final class RecordGrouperFactoryTest {

    @Test
    void topicPartition() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{start_offset}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertThat(RecordGrouperFactory.TOPIC_PARTITION_RECORD).isEqualTo(grType);
    }

    @Test
    void topicPartitionAndKey() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{key}}/{{start_offset}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertThat(RecordGrouperFactory.TOPIC_PARTITION_KEY_RECORD).isEqualTo(grType);
    }

    @Test
    void keyOnly() {
        final Template filenameTemplate = Template.of("{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertThat(RecordGrouperFactory.KEY_RECORD).isEqualTo(grType);
    }

    @Test
    void keyAndTopicPartition() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertThat(RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD).isEqualTo(grType);
    }

    public static Stream<Arguments> customGrouperTestData() {
        return Stream.of(
                Arguments.of(true, "avro", "{{topic}}/{{partition}}/{{start_offset}}", 100, TimestampSource.Type.EVENT),
                Arguments.of(true, "parquet", "{{topic}}/{{partition}}/{{start_offset}}", 100,
                        TimestampSource.Type.EVENT),
                Arguments.of(false, "csv", "{{key}}", 1, TimestampSource.Type.WALLCLOCK),
                Arguments.of(false, "csv", "{{foo}} {{bat}}", 1, TimestampSource.Type.WALLCLOCK));
    }
    @ParameterizedTest
    @MethodSource("customGrouperTestData")
    void customGrouper(final boolean schemaBased, final String format, final String template,
            final int maxRecordsPerFile, final TimestampSource.Type timestampSource) {

        final AivenCommonConfig configuration = new TestConfig.Builder().withMinimalProperties()
                .withProperty(AivenCommonConfig.CUSTOM_RECORD_GROUPER_BUILDER,
                        TestRecordGrouperBuilders.TestRecordGrouperBuilder.class.getName())
                .withProperty(AivenCommonConfig.FORMAT_OUTPUT_TYPE_CONFIG, format)
                .withProperty(AivenCommonConfig.FILE_NAME_TEMPLATE_CONFIG, template)
                .withProperty(AivenCommonConfig.FILE_MAX_RECORDS, String.valueOf(maxRecordsPerFile))
                .withProperty(AivenCommonConfig.FILE_NAME_TIMESTAMP_SOURCE, timestampSource.name())
                .build();

        final var grouper = RecordGrouperFactory.newRecordGrouper(configuration);
        assertThat(grouper).isInstanceOf(TestRecordGrouper.class);
        final TestRecordGrouper testGrouper = (TestRecordGrouper) grouper;

        assertThat(testGrouper.filenameTemplate.originalTemplate()).isEqualTo(template);
        assertThat(testGrouper.maxRecordsPerFile).isEqualTo(maxRecordsPerFile);
        assertThat(testGrouper.timestampSource.type()).isEqualTo(timestampSource);
        assertThat(testGrouper.schemaBased).isEqualTo(schemaBased);
        assertThat(testGrouper.config).isSameAs(configuration);
    }
}
