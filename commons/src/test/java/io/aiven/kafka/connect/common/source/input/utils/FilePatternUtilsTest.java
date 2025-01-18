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

package io.aiven.kafka.connect.common.source.input.utils;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Optional;

import io.aiven.kafka.connect.common.source.task.Context;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FilePatternUtilsTest {

    @ParameterizedTest
    @CsvSource({ "{{topic}}-1.txt,'topic', logs-1.txt, topic", "{{topic}}-{{partition}}.txt,, logs-1.txt, logs",
            "{{topic}}-{{partition}}.txt,, logs2-1.txt, logs2",
            "{{topic}}-{{partition}}.txt,anomaly, logs2-1.txt, anomaly" })
    void checkTopicDistribution(final String expectedSourceFormat, final String configuredTopic,
            final String sourceName, final String expectedTopic) {

        final FilePatternUtils utils = new FilePatternUtils(expectedSourceFormat, configuredTopic);
        final Optional<Context<String>> ctx = utils.process(sourceName);
        assertThat(ctx.isPresent()).isTrue();
        assertThat(ctx.get().getTopic().isPresent()).isTrue();
        assertThat(ctx.get().getTopic().get()).isEqualTo(expectedTopic);
    }

    @ParameterizedTest
    @CsvSource({ "{{topic}}-{{partition}}-{{start_offset}}.txt,, logs2-1-0001.txt, logs2,1,0001",
            "{{topic}}-{{start_offset}}-{{partition}}.txt,, logs2-0001-1.txt, logs2,0001,1",
            "{{topic}}-{{start_offset}}-{{partition}}.txt,, logs2-99999-1.txt, logs2,1,99999",
            "{{partition}}-{{start_offset}}-{{topic}}.txt,, logs2-1-logs2.txt, logs2,2,0001",
            "{{partition}}-{{start_offset}}-{{topic}}.txt,secondaryTopic, logs2-1-logs2.txt, secondaryTopic,2,0001", })
    void checkTopicDistribution(final String expectedSourceFormat, final String configuredTopic,
            final String sourceName, final String expectedTopic, final int expectedPartition,
            final int expectedOffset) {

        final FilePatternUtils utils = new FilePatternUtils(expectedSourceFormat, configuredTopic);
        final Optional<Context<String>> ctx = utils.process(sourceName);
        assertThat(ctx.isPresent()).isTrue();
        assertThat(ctx.get().getTopic().isPresent()).isTrue();
        assertThat(ctx.get().getTopic().get()).isEqualTo(expectedTopic);
        assertThat(ctx.get().getPartition().isPresent()).isTrue();
        assertThat(ctx.get().getPartition().get()).isEqualTo(expectedPartition);
        assertThat(ctx.get().getOffset().isPresent()).isTrue();
        assertThat(ctx.get().getOffset().get()).isEqualTo(expectedOffset);
    }

}
