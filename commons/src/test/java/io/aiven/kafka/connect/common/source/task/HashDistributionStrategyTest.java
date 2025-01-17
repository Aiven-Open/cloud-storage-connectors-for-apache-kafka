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
    final DistributionType strategy = DistributionType.OBJECT_HASH;
    @ParameterizedTest
    @CsvSource({ "logs-0-0002.txt", "logs-1-0002.txt", "logs-2-0002.txt", "logs-3-0002.txt", "logs-4-0002.txt",
            "logs-5-0002.txt", "logs-6-0002.txt", "logs-7-0002.txt", "logs-8-0002.txt", "logs-9-0002.txt",
            "logs-1-0002.txt", "logs-3-0002.txt", "logs-5-0002.txt", "value-6-0002.txt", "logs-7-0002.txt" })
    void hashDistributionExactlyOnce(final String path) {
        final int maxTaskId = 10;
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTaskId);
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
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
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
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
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

    @ParameterizedTest
    @CsvSource({ "-0", "-1", "-999", "-01", "-2002020" })
    void hashDistributionWithNegativeValues(final int hashCode) {
        final int maxTasks = 10;
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final FilePatternUtils utils = new FilePatternUtils(".*", "targetTopic");
        final Optional<Context<HashCodeKey>> ctx = utils.process(new HashCodeKey(hashCode));

        assertThat(ctx).isPresent();
        final int result = taskDistribution.getTaskFor(ctx.get());

        assertThat(result).isLessThan(maxTasks);
        assertThat(result).isGreaterThanOrEqualTo(0);

    }

    private Context<String> getContext(final String expectedSourceName, final String filename,
            final String targetTopic) {
        final FilePatternUtils utils = new FilePatternUtils(expectedSourceName, targetTopic);
        final Optional<Context<String>> ctx = utils.process(filename);
        assertThat(ctx.isPresent()).isTrue();
        // Hash distribution can have an empty context can have an empty context
        return ctx.get();
    }

    static class HashCodeKey {
        private final int hashCodeValue;
        public HashCodeKey(final int hashCodeValue) {
            this.hashCodeValue = hashCodeValue;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final HashCodeKey that = (HashCodeKey) other;
            return hashCodeValue == that.hashCodeValue;
        }

        @Override
        public int hashCode() {
            return hashCodeValue;
        }
    }
}
