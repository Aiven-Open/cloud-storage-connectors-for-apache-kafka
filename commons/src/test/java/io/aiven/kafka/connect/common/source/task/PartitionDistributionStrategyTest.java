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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class PartitionDistributionStrategyTest {

    @Test
    void partitionInFileNameDefaultAivenS3Sink() {
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(2);
        assertThat(taskDistribution.isPartOfTask(1, "logs-1-00112.gz",
                FilePatternUtils.configurePattern("{{topic}}-{{partition}}-{{start_offset}}"))).isTrue();
    }

    @Test
    void partitionLocationNotSetExpectException() {
        assertThatThrownBy(() -> new PartitionDistributionStrategy(2).isPartOfTask(1, "",
                FilePatternUtils.configurePattern("logs-23-<partition>-<start_offset>")))
                .isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Source name format logs-23-<partition>-<start_offset> missing partition pattern {{partition}} please configure the expected source to include the partition pattern.");

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
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(1);
        // This test is testing the filename matching not the task allocation.
        assertThat(taskDistribution.isPartOfTask(0, filename,
                FilePatternUtils.configurePattern(configuredFilenamePattern))).isTrue();
    }

    @ParameterizedTest(name = "[{index}] Pattern: {0}, Filename: {1}")
    @CsvSource({ "different-topic-{{partition}}-{{start_offset}},logs-1-00112.gz",
            "no-seperator-in-date-partition-offset-{{timestamp}}-{{partition}}-{{start_offset}},no-seperator-in-date-partition-offset-202420220201100112.gz",
            "logs-2024-{{timestamp}}-{{partition}}-{{start_offset}},logs-20201-1-00112.gz",
            "logs-2024-{{timestamp}}{{partition}}-{{start_offset}},logs-202011-00112.gz",
            "logs-2024-{{timestamp}}{{partition}}-{{start_offset}}, ",
            "logs-2023-{{partition}}-{{start_offset}},logs-2023-one-00112.gz" })
    void expectFalseOnMalformedFilenames(final String configuredFilenamePattern, final String filename) {
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(1);
        // This test is testing the filename matching not the task allocation.
        assertThat(taskDistribution.isPartOfTask(0, filename,
                FilePatternUtils.configurePattern(configuredFilenamePattern))).isFalse();
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1}, Filename: {1}")
    @CsvSource({ "0,10,topics/logs/0/logs-0-0002.txt", "1,10,topics/logs/1/logs-1-0002.txt",
            "2,10,topics/logs/2/logs-2-0002.txt", "3,10,topics/logs/3/logs-3-0002.txt",
            "4,10,topics/logs/4/logs-4-0002.txt", "5,10,topics/logs/5/logs-5-0002.txt",
            "6,10,topics/logs/6/logs-6-0002.txt", "7,10,topics/logs/7/logs-7-0002.txt",
            "8,10,topics/logs/8/logs-8-0002.txt", "9,10,topics/logs/9/logs-9-0002.txt" })
    void checkCorrectDistributionAcrossTasksOnFileName(final int taskId, final int maxTasks, final String path) {

        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);

        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("logs-{{partition}}-{{start_offset}}"))).isTrue();
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {0}, Filename: {1}")
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.txt", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void filenameDistributionExactlyOnceDistribution(final int maxTasks, final String path) {

        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern("logs-{{partition}}.txt")));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {0}, TaskId: {1}, Filename: {2}")
    @CsvSource({ "10,5,topics/logs/0/logs-0002.txt", "10,5,topics/logs/1/logs-001.txt",
            "10,5,topics/logs/2/logs-0002.txt", "10,5,topics/logs/3/logs-0002.txt", "10,5,topics/logs/4/logs-0002.txt",
            "10,5,topics/logs/5/logs-0002.txt", "10,5,topics/logs/6/logs-0002.txt", "10,5,topics/logs/7/logs-0002.txt",
            "10,5,topics/logs/8/logs-0002.txt", "10,5,topics/logs/9/logs-0002.txt" })
    void filenameDistributionExactlyOnceDistributionWithTaskReconfiguration(final int maxTasks,
            final int maxTaskAfterReConfig, final String path) {

        final String expectedSourceNameFormat = "logs-{{partition}}.txt";
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern(expectedSourceNameFormat)));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
        taskDistribution.configureDistributionStrategy(maxTaskAfterReConfig);

        results.clear();
        for (int taskId = 0; taskId < maxTaskAfterReConfig; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern(expectedSourceNameFormat)));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE);
    }

    @ParameterizedTest
    @CsvSource({
            "{topic}}-1.txt,'Source name format {topic}}-1.txt missing partition pattern {{partition}} please configure the expected source to include the partition pattern.'",
            " ,'Source name format null missing partition pattern {{partition}} please configure the expected source to include the partition pattern.'",
            "empty-pattern,'Source name format empty-pattern missing partition pattern {{partition}} please configure the expected source to include the partition pattern.'" })
    void malformedFilenameSetup(final String expectedSourceFormat, final String expectedErrorMessage) {
        final int maxTaskId = 1;
        assertThatThrownBy(() -> new PartitionDistributionStrategy(maxTaskId).isPartOfTask(1, "",
                FilePatternUtils.configurePattern(expectedSourceFormat))).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void errorExpectedNullGivenForSourceNameFormat() {
        final int maxTaskId = 1;
        assertThatThrownBy(() -> new PartitionDistributionStrategy(maxTaskId).isPartOfTask(1, "",
                FilePatternUtils.configurePattern(null))).isInstanceOf(ConfigException.class)
                .hasMessage("Source name format null missing partition pattern {{partition}} please configure"
                        + " the expected source to include the partition pattern.");
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,1,topics/logs/partition=5/logs+5+0002.txt,true",
            "0,4,topics/logs/partition=5/logs+5+0002.txt,false", "1,4,topics/logs/partition=5/logs+5+0002.txt,true",
            "0,3,topics/logs/partition=5/logs+5+0002.txt,false", "0,5,topics/logs/partition=5/logs+5+0002.txt,true",
            "2,3,topics/logs/partition=5/logs+5+0002.txt,true" })
    void withLeadingStringPartitionNamingConvention(final int taskId, final int maxTasks, final String path,
            final boolean expectedResult) {

        final PartitionDistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);

        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("topics/{{topic}}/partition={{partition}}/.*$")))
                .isEqualTo(expectedResult);
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,1,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "0,4,bucket/topics/topic-1/5/logs+5+0002.txt,false", "1,4,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "0,3,bucket/topics/topic-1/5/logs+5+0002.txt,false", "0,5,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "2,3,bucket/topics/topic-1/5/logs+5+0002.txt,true" })
    void partitionInPathConvention(final int taskId, final int maxTaskId, final String path,
            final boolean expectedResult) {

        final PartitionDistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTaskId);

        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("bucket/topics/{{topic}}/{{partition}}/.*$")))
                .isEqualTo(expectedResult);
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,10,topics/logs/0/logs-0002.txt", "1,10,topics/logs/1/logs-0002.txt",
            "2,10,topics/logs/2/logs-0002.txt", "3,10,topics/logs/3/logs-0002.txt", "4,10,topics/logs/4/logs-0002.txt",
            "5,10,topics/logs/5/logs-0002.txt", "6,10,topics/logs/6/logs-0002.txt", "7,10,topics/logs/7/logs-0002.txt",
            "8,10,topics/logs/8/logs-0002.txt", "9,10,topics/logs/9/logs-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTaskId, final String path) {

        final PartitionDistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTaskId);

        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("topics/{{topic}}/{{partition}}/.*$"))).isTrue();
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "1,10,topcs/logs/0/logs-0002.txt", "2,10,topics/logs/1", "3,10,S3/logs/2/logs-0002.txt",
            "4,10,topics/log/3/logs-0002.txt", "5,10,prod/logs/4/logs-0002.txt", "6,10,misspelt/logs/5/logs-0002.txt",
            "7,10,test/logs/6/logs-0002.txt", "8,10,random/logs/7/logs-0002.txt", "9,10,DEV/logs/8/logs-0002.txt",
            "10,10,poll/logs/9/logs-0002.txt" })
    void expectNoMatchOnUnconfiguredPaths(final int taskId, final int maxTaskId, final String path) {

        final PartitionDistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTaskId);

        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("topics/{{topic}}/{{partition}}/.*$"))).isFalse();
    }

    @Test
    void expectExceptionOnNonIntPartitionSupplied() {
        final int taskId = 1;
        final int maxTaskId = 1;
        final String path = "topics/logs/one/test-001.txt";

        final PartitionDistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTaskId);
        assertThat(taskDistribution.isPartOfTask(taskId, path,
                FilePatternUtils.configurePattern("topics/{{topic}}/{{partition}}/.*$"))).isFalse();
    }

    @Test
    void malformedRegexSetup() {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionDistributionStrategy(maxTaskId).isPartOfTask(1, "",
                FilePatternUtils.configurePattern("topics/{{topic}}/"))).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Source name format topics/{{topic}}/ missing partition pattern {{partition}} please configure the expected source to include the partition pattern.");
    }

    @ParameterizedTest
    @CsvSource({
            ",Source name format null missing partition pattern {{partition}} please configure the expected source to include the partition pattern.",
            "@adsfs,Source name format @adsfs missing partition pattern {{partition}} please configure the expected source to include the partition pattern.",
            "empty-path,Source name format empty-path missing partition pattern {{partition}} please configure the expected source to include the partition pattern." })
    void malformedPathSetup(final String expectedPathFormat, final String expectedErrorMessage) {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionDistributionStrategy(maxTaskId).isPartOfTask(1, expectedPathFormat,
                FilePatternUtils.configurePattern(expectedPathFormat))).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.log", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void partitionPathDistributionExactlyOnceDistribution(final int maxTasks, final String path) {
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern("topics/{{topic}}/{{partition}}/.*$")));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
    }

    @ParameterizedTest
    @CsvSource({ "10,5,topics/logs/0/logs-0002.txt", "10,5,topics/logs/1/logs-001.txt",
            "10,5,topics/logs/2/logs-0002.txt", "10,5,topics/logs/3/logs-0002.txt", "10,5,topics/logs/4/logs-0002.txt",
            "10,5,topics/logs/5/logs-0002.txt", "10,5,topics/logs/6/logs-0002.txt", "10,5,topics/logs/7/logs-0002.txt",
            "10,5,topics/logs/8/logs-0002.txt", "10,5,topics/logs/9/logs-0002.txt" })
    void partitionPathDistributionExactlyOnceDistributionWithTaskReconfiguration(final int maxTasks,
            final int maxTaskAfterReConfig, final String path) {

        final String expectedSourceNameFormat = "topics/{{topic}}/{{partition}}/.*$";
        final DistributionStrategy taskDistribution = new PartitionDistributionStrategy(maxTasks);
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern(expectedSourceNameFormat)));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
        taskDistribution.configureDistributionStrategy(maxTaskAfterReConfig);

        results.clear();
        for (int taskId = 0; taskId < maxTaskAfterReConfig; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path,
                    FilePatternUtils.configurePattern(expectedSourceNameFormat)));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE);
    }

}
