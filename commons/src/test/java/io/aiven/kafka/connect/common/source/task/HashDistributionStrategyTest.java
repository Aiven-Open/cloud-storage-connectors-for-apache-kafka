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

package io.aiven.kafka.connect.common.source.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class HashDistributionStrategyTest {

    @ParameterizedTest
    @CsvSource({ "logs-0-0002.txt", "logs-1-0002.txt", "logs-2-0002.txt", "logs-3-0002.txt", "logs-4-0002.txt",
            "logs-5-0002.txt", "logs-6-0002.txt", "logs-7-0002.txt", "logs-8-0002.txt", "logs-9-0002.txt",
            "logs-1-0002.txt", "logs-3-0002.txt", "logs-5-0002.txt", "value-6-0002.txt", "logs-7-0002.txt" })
    void hashDistributionExactlyOnce(final String path) {
        final int maxTaskId = 10;
        final DistributionStrategy taskDistribution = new HashDistributionStrategy(maxTaskId);
        final Context<String> ctx = getContext("{{topic}}-{{partition}}-{{start_offset}}", path, null);

        final List<Integer> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTaskId; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @ParameterizedTest
    @CsvSource({ "logs-0-0002.txt", "logs-1-0002.txt", "logs-2-0002.txt", "logs-3-0002.txt", "logs-4-0002.txt",
            "logs-5-0002.txt", "logs-6-0002.txt", "logs-7-0002.txt", "logs-8-0002.txt", "logs-9-0002.txt",
            "logs-1-0002.txt", "logs-3-0002.txt", "logs-5-0002.txt", "value-6-0002.txt", "logs-7-0002.txt" })
    void hashDistributionExactlyOnceWithReconfigureEvent(final String path) {
        int maxTasks = 10;
        final DistributionStrategy taskDistribution = new HashDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext("{{topic}}-{{partition}}-{{start_offset}}", path, null);

        final List<Integer> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
        results.clear();
        maxTasks = 5;
        taskDistribution.configureDistributionStrategy(maxTasks);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @ParameterizedTest
    @CsvSource({ "key-0.txt", "key-0002.txt", "key-0002.txt", "anImage8-0002.png",
            "reallylongfilenamecreatedonS3tohisdesomedata and alsohassome spaces.txt" })
    void hashDistributionExactlyOnceWithReconfigureEventAndMatchAllExpectedSource(final String path) {
        int maxTasks = 10;
        final DistributionStrategy taskDistribution = new HashDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext(".*", path, "topic1");

        final List<Integer> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
        results.clear();
        maxTasks = 5;
        taskDistribution.configureDistributionStrategy(maxTasks);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    private Context<String> getContext(final String expectedSourceName, final String filename,
            final String targetTopic) {
        final FilePatternUtils<String> utils = new FilePatternUtils<>(expectedSourceName, targetTopic);
        final Optional<Context<String>> ctx = utils.process(filename);
        assertThat(ctx.isPresent()).isTrue();
        // Hash distribution can have an empty context can have an empty context
        return ctx.get();
    }
}
