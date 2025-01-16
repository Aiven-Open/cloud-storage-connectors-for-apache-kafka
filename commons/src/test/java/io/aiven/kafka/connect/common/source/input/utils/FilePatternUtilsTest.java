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

        final FilePatternUtils<String> utils = new FilePatternUtils<>(expectedSourceFormat, configuredTopic);
        final Optional<Context<String>> ctx = utils.process(sourceName);
        assertThat(ctx.isPresent()).isTrue();
        assertThat(ctx.get().getTopic().isPresent()).isTrue();
        assertThat(ctx.get().getTopic().get()).isEqualTo(expectedTopic);
    }

}
