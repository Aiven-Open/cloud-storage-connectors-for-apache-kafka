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
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class PartitionInPathDistributionStrategyTest {

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,1,topics/logs/partition=5/logs+5+0002.txt,true",
            "0,4,topics/logs/partition=5/logs+5+0002.txt,false", "1,4,topics/logs/partition=5/logs+5+0002.txt,true",
            "0,3,topics/logs/partition=5/logs+5+0002.txt,false", "0,5,topics/logs/partition=5/logs+5+0002.txt,true",
            "2,3,topics/logs/partition=5/logs+5+0002.txt,true" })
    void withLeadingStringPartitionNamingConvention(final int taskId, final int maxTasks, final String path,
            final boolean expectedResult) {

        final PartitionInPathDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTasks,
                "topics/logs/partition=\\{\\{partition}}/");

        assertThat(taskDistribution.isPartOfTask(taskId, path)).isEqualTo(expectedResult);
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,1,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "0,4,bucket/topics/topic-1/5/logs+5+0002.txt,false", "1,4,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "0,3,bucket/topics/topic-1/5/logs+5+0002.txt,false", "0,5,bucket/topics/topic-1/5/logs+5+0002.txt,true",
            "2,3,bucket/topics/topic-1/5/logs+5+0002.txt,true" })
    void partitionInPathConvention(final int taskId, final int maxTaskId, final String path,
            final boolean expectedResult) {

        final PartitionInPathDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTaskId,
                "bucket/topics/topic-1/\\{\\{partition}}/");

        assertThat(taskDistribution.isPartOfTask(taskId, path)).isEqualTo(expectedResult);
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "0,10,topics/logs/0/logs-0002.txt", "1,10,topics/logs/1/logs-0002.txt",
            "2,10,topics/logs/2/logs-0002.txt", "3,10,topics/logs/3/logs-0002.txt", "4,10,topics/logs/4/logs-0002.txt",
            "5,10,topics/logs/5/logs-0002.txt", "6,10,topics/logs/6/logs-0002.txt", "7,10,topics/logs/7/logs-0002.txt",
            "8,10,topics/logs/8/logs-0002.txt", "9,10,topics/logs/9/logs-0002.txt" })
    void checkCorrectDistributionAcrossTasks(final int taskId, final int maxTaskId, final String path) {

        final PartitionInPathDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTaskId,
                "topics/logs/\\{\\{partition}}/");

        assertThat(taskDistribution.isPartOfTask(taskId, path)).isTrue();
    }

    @ParameterizedTest(name = "[{index}] TaskId: {0}, MaxTasks: {1} Filename: {2}")
    @CsvSource({ "1,10,topcs/logs/0/logs-0002.txt", "2,10,topics/logs/1", "3,10,S3/logs/2/logs-0002.txt",
            "4,10,topics/log/3/logs-0002.txt", "5,10,prod/logs/4/logs-0002.txt", "6,10,misspelt/logs/5/logs-0002.txt",
            "7,10,test/logs/6/logs-0002.txt", "8,10,random/logs/7/logs-0002.txt", "9,10,DEV/logs/8/logs-0002.txt",
            "10,10,poll/logs/9/logs-0002.txt" })
    void expectNoMatchOnUnconfiguredPaths(final int taskId, final int maxTaskId, final String path) {

        final PartitionInPathDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTaskId,
                "topics/logs/\\{\\{partition}}/");

        assertThat(taskDistribution.isPartOfTask(taskId, path)).isFalse();
    }

    @Test
    void expectExceptionOnNonIntPartitionSupplied() {
        final int taskId = 1;
        final int maxTaskId = 1;
        final String path = "topics/logs/one/test-001.txt";

        final PartitionInPathDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTaskId,
                "topics/logs/\\{\\{partition}}/");
        assertThatThrownBy(() -> taskDistribution.isPartOfTask(taskId, path)).isInstanceOf(ConnectException.class)
                .hasMessage(
                        "Unexpected non integer value found parsing path for partitionId: topics/logs/one/test-001.txt");
    }

    @Test
    void malformedRegexSetup() {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionInPathDistributionStrategy(maxTaskId, "topics/logs/{{partition}}/"))
                .isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Expected path format topics/logs/{{partition}}/ is missing the identifier '\\{\\{partition}}' to correctly select the partition");
    }

    @ParameterizedTest
    @CsvSource({
            ",Expected path format null is missing the identifier '\\{\\{partition}}' to correctly select the partition",
            "@adsfs,Expected path format @adsfs is missing the identifier '\\{\\{partition}}' to correctly select the partition",
            "empty-path,Expected path format empty-path is missing the identifier '\\{\\{partition}}' to correctly select the partition" })
    void malformedPathSetup(final String expectedPathFormat, final String expectedErrorMessage) {
        final int maxTaskId = 1;

        assertThatThrownBy(() -> new PartitionInPathDistributionStrategy(maxTaskId, expectedPathFormat))
                .isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @CsvSource({ "10,topics/logs/0/logs-0002.txt", "10,topics/logs/1/logs-001.log", "10,topics/logs/2/logs-0002.txt",
            "10,topics/logs/3/logs-0002.txt", "10,topics/logs/4/logs-0002.txt", "10,topics/logs/5/logs-0002.txt",
            "10,topics/logs/6/logs-0002.txt", "10,topics/logs/7/logs-0002.txt", "10,topics/logs/8/logs-0002.txt",
            "10,topics/logs/9/logs-0002.txt" })
    void partitionPathDistributionExactlyOnceDistribution(final int maxTasks, final String path) {

        final ObjectDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTasks,
                "topics/logs/\\{\\{partition}}");
        final List<Boolean> results = new ArrayList<>();
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            results.add(taskDistribution.isPartOfTask(taskId, path));
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

        final String expectedSourceNameFormat = "topics/logs/\\{\\{partition}}";
        final ObjectDistributionStrategy taskDistribution = new PartitionInPathDistributionStrategy(maxTasks,
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

}
