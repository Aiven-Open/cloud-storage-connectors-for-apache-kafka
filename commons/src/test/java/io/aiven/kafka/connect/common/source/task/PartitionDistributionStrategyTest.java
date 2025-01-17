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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class PartitionDistributionStrategyTest {
    final DistributionType strategy = DistributionType.PARTITION;
    @Test
    void partitionInFileNameDefaultAivenS3Sink() {
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(2);
        final Context<String> ctx = getContext("{{topic}}-{{partition}}-{{start_offset}}", "logs-1-00112.gz");
        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{index}] Pattern: {0}, Filename: {1}")
    @CsvSource({ "{{topic}}-{{partition}}-{{start_offset}},logs-0-00112.gz",
            "{{topic}}-2024-{{timestamp}}-{{partition}}-{{start_offset}},logs-2024-20220201-0-00112.gz",
            "{{topic}}-2023-{{partition}}-{{start_offset}},logs-2023-0-00112.gz",
            "logs-2023-{{partition}}-{{start_offset}},logs-2023-0-00112.gz",
            "{{topic}}-{{timestamp}}-{{timestamp}}-{{timestamp}}-{{partition}}-{{start_offset}},logs1-2022-10-02-10-00112.gz",
            "{{topic}}{{partition}}-{{start_offset}},89521-00112.gz",
            "{{topic}}-{{partition}},Emergency-TEST1-00112.gz",
            "Emergency-TEST1-{{partition}},Emergency-TEST1-00112.gz",
            "{{topic}}-{{partition}}-{{start_offset}},PROD-logs-1-00112.gz",
            "{{topic}}-{{partition}},DEV_team_1-00112.gz",
            "{{topic}}-{{partition}}-{{start_offset}},timeseries-1-00112.gz" })
    void testPartitionFileNamesAndExpectedOutcomes(final String configuredFilenamePattern, final String filename) {
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(1);
        // This test is testing the filename matching not the task allocation.
        final Context<String> ctx = getContext(configuredFilenamePattern, filename);
        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(0);
    }

    @ParameterizedTest(name = "[{index}] Pattern: {0}, Filename: {1}")
    @CsvSource({ "different-topic-{{partition}}-{{start_offset}},logs-1-00112.gz",
            "no-seperator-in-date-partition-offset-{{timestamp}}-{{partition}}-{{start_offset}},no-seperator-in-date-partition-offset-202420220201100112.gz",
            "logs-2024-{{timestamp}}-{{partition}}-{{start_offset}},logs-20201-1-00112.gz",
            "logs-2024-{{timestamp}}{{partition}}-{{start_offset}},logs-202011-00112.gz",
            "logs-2023-{{partition}}-{{start_offset}},logs-2023-one-00112.gz" })
    void expectFalseOnMalformedFilenames(final String configuredFilenamePattern, final String filename) {
        // This test is testing the filename matching not the task allocation.
        final Optional<Context<String>> ctx = getOptionalContext(configuredFilenamePattern, filename);
        assertThat(ctx).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1}, Filename: {1}")
    @CsvSource({ "0,10,topics/logs/0/logs-0-0002.txt", "1,10,topics/logs/1/logs-1-0002.txt",
            "2,10,topics/logs/2/logs-2-0002.txt", "3,10,topics/logs/3/logs-3-0002.txt",
            "4,10,topics/logs/4/logs-4-0002.txt", "5,10,topics/logs/5/logs-5-0002.txt",
            "6,10,topics/logs/6/logs-6-0002.txt", "7,10,topics/logs/7/logs-7-0002.txt",
            "8,10,topics/logs/8/logs-8-0002.txt", "9,10,topics/logs/9/logs-9-0002.txt" })
    void checkCorrectDistributionAcrossTasksOnFileName(final int taskId, final int maxTasks, final String path) {

        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext("logs-{{partition}}-{{start_offset}}", path);
        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(taskId);
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {0}, Filename: {1}")
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.txt", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void filenameDistributionExactlyOnceDistribution(final int maxTasks, final String path) {

        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final List<Integer> results = new ArrayList<>();
        final Context<String> ctx = getContext("logs-{{partition}}.txt", path);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        // TODO Double check this, they should all match the first task.
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {0}, TaskId: {1}, Filename: {2}")
    @CsvSource({ "10,5,topics/logs/0/logs-0002.txt", "10,5,topics/logs/1/logs-001.txt",
            "10,5,topics/logs/2/logs-0002.txt", "10,5,topics/logs/3/logs-0002.txt", "10,5,topics/logs/4/logs-0002.txt",
            "10,5,topics/logs/5/logs-0002.txt", "10,5,topics/logs/6/logs-0002.txt", "10,5,topics/logs/7/logs-0002.txt",
            "10,5,topics/logs/8/logs-0002.txt", "10,5,topics/logs/9/logs-0002.txt" })
    void filenameDistributionExactlyOnceDistributionWithTaskReconfiguration(final int maxTasks,
            final int maxTaskAfterReConfig, final String path) {

        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext("logs-{{partition}}.txt", path);

        final List<Integer> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
        taskDistribution.configureDistributionStrategy(maxTaskAfterReConfig);

        results.clear();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @ParameterizedTest
    @CsvSource({ "10,5,topics/logs/0/logs-0002.txt", "10,5,topics/logs/1/logs-001.txt",
            "10,5,topics/logs/2/logs-0002.txt", "10,5,topics/logs/3/logs-0002.txt", "10,5,topics/logs/4/logs-0002.txt",
            "10,5,topics/logs/5/logs-0002.txt", "10,5,topics/logs/6/logs-0002.txt", "10,5,topics/logs/7/logs-0002.txt",
            "10,5,topics/logs/8/logs-0002.txt", "10,5,topics/logs/9/logs-0002.txt" })
    void partitionPathDistributionExactlyOnceDistributionWithTaskReconfiguration(final int maxTasks,
            final int maxTaskAfterReConfig, final String path) {

        final String expectedSourceNameFormat = "topics/{{topic}}/{{partition}}/.*$";
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext(expectedSourceNameFormat, path);
        final List<Integer> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
        taskDistribution.configureDistributionStrategy(maxTaskAfterReConfig);

        results.clear();
        for (int taskId = 0; taskId < maxTaskAfterReConfig; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @ParameterizedTest
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.log", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void partitionPathDistributionExactlyOnceDistribution(final int maxTasks, final String path) {
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final List<Integer> results = new ArrayList<>();
        final Context<String> ctx = getContext("topics/{{topic}}/{{partition}}/.*$", path);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.getTaskFor(ctx));
        }
        assertThat(results).allMatch(i -> i == taskDistribution.getTaskFor(ctx));
    }

    @Test
    void expectEmptyContextOnNonIntPartitionSuppliedAsNoMatchOccurs() {
        final String path = "topics/logs/one/test-001.txt";
        final Optional<Context<String>> ctx = getOptionalContext("topics/{{topic}}/{{partition}}/.*$", path);
        assertThat(ctx).isEmpty();
    }

    @ParameterizedTest(name = "[{index}]  Filename: {2}")
    @CsvSource({ "topcs/logs/0/logs-0002.txt", "topics/logs/1", "S3/logs/2/logs-0002.txt",
            "topicss/log/3/logs-0002.txt", "prod/logs/4/logs-0002.txt", "misspelt/logs/5/logs-0002.txt",
            "test/logs/6/logs-0002.txt", "random/logs/7/logs-0002.txt", "DEV/logs/8/logs-0002.txt",
            "poll/logs/9/logs-0002.txt" })
    void expectNoMatchOnUnconfiguredPaths(final String path) {
        final Optional<Context<String>> ctx = getOptionalContext("topics/{{topic}}/{{partition}}/.*$", path);
        assertThat(ctx).isEmpty();
    }
    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,10,topics/logs/0/logs-0002.txt", "1,10,topics/logs/1/logs-0002.txt",
            "2,10,topics/logs/2/logs-0002.txt", "3,10,topics/logs/3/logs-0002.txt", "4,10,topics/logs/4/logs-0002.txt",
            "5,10,topics/logs/5/logs-0002.txt", "6,10,topics/logs/6/logs-0002.txt", "7,10,topics/logs/7/logs-0002.txt",
            "8,10,topics/logs/8/logs-0002.txt", "9,10,topics/logs/9/logs-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTaskId, final String path) {
        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTaskId);
        final Context<String> ctx = getContext("topics/{{topic}}/{{partition}}/.*$", path);
        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(taskId);
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {1} Filename: {2}")
    @CsvSource({ "1,bucket/topics/topic-1/5/logs+5+0002.txt,0", "4,bucket/topics/topic-1/5/logs+5+0002.txt,1",
            "4,bucket/topics/topic-1/5/logs+5+0002.txt,1", "3,bucket/topics/topic-1/5/logs+5+0002.txt,2",
            "5,bucket/topics/topic-1/5/logs+5+0002.txt,0", "3,bucket/topics/topic-1/5/logs+5+0002.txt,2" })
    void partitionInPathConvention(final int maxTaskId, final String path, final int expectedResult) {

        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTaskId);
        final Context<String> ctx = getContext("bucket/topics/{{topic}}/{{partition}}/.*$", path);
        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(expectedResult);
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {1} Filename: {2}")
    @CsvSource({ "1,topics/logs/partition=5/logs+5+0002.txt,0", "4,topics/logs/partition=5/logs+5+0002.txt,1",
            "4,topics/logs/partition=5/logs+5+0002.txt,1", "3,topics/logs/partition=5/logs+5+0002.txt,2",
            "5,topics/logs/partition=5/logs+5+0002.txt,0", "3,topics/logs/partition=5/logs+5+0002.txt,2" })
    void withLeadingStringPartitionNamingConvention(final int maxTasks, final String path, final int expectedResult) {

        final DistributionStrategy taskDistribution = strategy.getDistributionStrategy(maxTasks);
        final Context<String> ctx = getContext("topics/{{topic}}/partition={{partition}}/.*$", path);

        assertThat(taskDistribution.getTaskFor(ctx)).isEqualTo(expectedResult);
    }

    public static Context<String> getContext(final String configuredFilenamePattern, final String filename) {
        final Optional<Context<String>> ctx = getOptionalContext(configuredFilenamePattern, filename);
        assertThat(ctx.isPresent()).isTrue();
        return ctx.get();
    }

    public static Optional<Context<String>> getOptionalContext(final String configuredFilenamePattern,
            final String filename) {
        final FilePatternUtils<String> utils = new FilePatternUtils<>(configuredFilenamePattern, null);
        return utils.process(filename);
    }

}
