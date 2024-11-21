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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class PartitionInFilenameDistributionStrategyTest {

    @Test
    void partitionInFileNameDefaultAivenS3Sink() {
        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(2,
                "logs-\\{\\{partition}}-\\{\\{start_offset}}");
        assertThat(taskDistribution.isPartOfTask(1, "logs-1-00112.gz")).isTrue();
    }

    @Test
    void partitionLocationNotSetExpectException() {
        assertThatThrownBy(() -> new PartitionInFilenameDistributionStrategy(2, "logs-23-<partition>-<start_offset>"))
                .isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Source name format logs-23-<partition>-<start_offset> missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.");

    }

    @ParameterizedTest(name = "[{index}] Pattern: {0}, Filename: {1}")
    @CsvSource({ "logs-\\{\\{partition}}-\\{\\{start_offset}},logs-0-00112.gz",
            "logs-2024-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs-2024-20220201-0-00112.gz",
            "logs-2023-\\{\\{partition}}-\\{\\{start_offset}},logs-2023-0-00112.gz",
            "logs1-\\{\\{timestamp}}-\\{\\{timestamp}}-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs1-2022-10-02-10-00112.gz",
            "8952\\{\\{partition}}-\\{\\{start_offset}},89521-00112.gz",
            "Emergency-TEST\\{\\{partition}}-\\{\\{start_offset}},Emergency-TEST1-00112.gz",
            "PROD-logs-\\{\\{partition}}-\\{\\{start_offset}},PROD-logs-1-00112.gz",
            "DEV_team_\\{\\{partition}}-\\{\\{start_offset}},DEV_team_1-00112.gz",
            "timeseries-\\{\\{partition}}-\\{\\{start_offset}},timeseries-1-00112.gz" })
    void testPartitionFileNamesAndExpectedOutcomes(final String configuredFilenamePattern, final String filename) {
        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(1,
                configuredFilenamePattern);
        // This test is testing the filename matching not the task allocation.
        assertThat(taskDistribution.isPartOfTask(0, filename)).isTrue();
    }

    @ParameterizedTest(name = "[{index}] Pattern: {0}, Filename: {1}")
    @CsvSource({ "different-topic-\\{\\{partition}}-\\{\\{start_offset}},logs-1-00112.gz",
            "no-seperator-in-date-partition-offset-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},no-seperator-in-date-partition-offset-202420220201100112.gz",
            "logs-2024-\\{\\{timestamp}}-\\{\\{partition}}-\\{\\{start_offset}},logs-20201-1-00112.gz",
            "logs-2024-\\{\\{timestamp}}\\{\\{partition}}-\\{\\{start_offset}},logs-202011-00112.gz",
            "logs-2024-\\{\\{timestamp}}\\{\\{partition}}-\\{\\{start_offset}}, ",
            "logs-2023-\\{\\{partition}}-\\{\\{start_offset}},logs-2023-one-00112.gz" })
    void expectFalseOnMalformedFilenames(final String configuredFilenamePattern, final String filename) {
        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(1,
                configuredFilenamePattern);
        // This test is testing the filename matching not the task allocation.
        assertThat(taskDistribution.isPartOfTask(0, filename)).isFalse();
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1}, Filename: {1}")
    @CsvSource({ "0,10,topics/logs/0/logs-0-0002.txt", "1,10,topics/logs/1/logs-1-0002.txt",
            "2,10,topics/logs/2/logs-2-0002.txt", "3,10,topics/logs/3/logs-3-0002.txt",
            "4,10,topics/logs/4/logs-4-0002.txt", "5,10,topics/logs/5/logs-5-0002.txt",
            "6,10,topics/logs/6/logs-6-0002.txt", "7,10,topics/logs/7/logs-7-0002.txt",
            "8,10,topics/logs/8/logs-8-0002.txt", "9,10,topics/logs/9/logs-9-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTasks, final String path) {

        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(maxTasks,
                "logs-\\{\\{partition}}-\\{\\{start_offset}}");

        assertThat(taskDistribution.isPartOfTask(taskId, path)).isTrue();
    }

    @ParameterizedTest(name = "[{index}] MaxTasks: {0}, Filename: {1}")
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.txt", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void filenameDistributionExactlyOnceDistribution(final int maxTasks, final String path) {

        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(maxTasks,
                "logs-\\{\\{partition}}.txt");
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path));
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

        final String expectedSourceNameFormat = "logs-\\{\\{partition}}.txt";
        final ObjectDistributionStrategy taskDistribution = new PartitionInFilenameDistributionStrategy(maxTasks,
                expectedSourceNameFormat);
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
        taskDistribution.reconfigureDistributionStrategy(maxTaskAfterReConfig, expectedSourceNameFormat);

        results.clear();
        for (int taskId = 0; taskId < maxTaskAfterReConfig; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path));
        }
        assertThat(results).containsExactlyInAnyOrder(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
                Boolean.FALSE);
    }

    @ParameterizedTest
    @CsvSource({
            "logs-{{partition}}.txt,'Source name format logs-{{partition}}.txt missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.'",
            " ,'Source name format null missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.'",
            "empty-pattern,'Source name format empty-pattern missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.'" })
    void malformedFilenameSetup(final String expectedSourceFormat, final String expectedErrorMessage) {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionInFilenameDistributionStrategy(maxTaskId, expectedSourceFormat))
                .isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void errorExpectedNullGivenForSourceNameFormat() {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionInFilenameDistributionStrategy(maxTaskId, null))
                .isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Source name format null missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.");
    }

}
