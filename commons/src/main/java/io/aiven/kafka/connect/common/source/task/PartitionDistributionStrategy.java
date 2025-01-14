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

import java.util.Optional;
import java.util.regex.Pattern;

import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PartitionDistributionStrategy} finds a partition in the object's filename by matching it to an expected
 * format, and assigns all partitions to the same task.
 * <p>
 * This useful when a sink connector has created the object name in a format like
 * {@code topicname-{{partition}}-{{start_offset}}}, and we want all objects with the same partition to be processed
 * within a single task.
 */
public final class PartitionDistributionStrategy implements DistributionStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionDistributionStrategy.class);
    private int maxTasks;

    public PartitionDistributionStrategy(final int maxTasks) {
        this.maxTasks = maxTasks;
    }

    /**
     *
     * @param sourceNameToBeEvaluated
     *            is the filename/table name of the source for the connector.
     * @return Predicate to confirm if the given source name matches
     */
    @Override
    public boolean isPartOfTask(final int taskId, final String sourceNameToBeEvaluated, final Pattern filePattern) {
        if (sourceNameToBeEvaluated == null) {
            LOG.warn("Ignoring as it is not passing a correct filename to be evaluated.");
            return false;
        }
        final Optional<Integer> optionalPartitionId = FilePatternUtils.getPartitionId(filePattern,
                sourceNameToBeEvaluated);

        if (optionalPartitionId.isPresent()) {
            return optionalPartitionId.get() < maxTasks
                    ? taskMatchesPartition(taskId, optionalPartitionId.get())
                    : taskMatchesPartition(taskId, optionalPartitionId.get() % maxTasks);
        }
        LOG.warn("Unable to find the partition from this file name {}", sourceNameToBeEvaluated);
        return false;
    }

    boolean taskMatchesPartition(final int taskId, final int partitionId) {
        // The partition id and task id are both expected to start at 0 but if the task id is changed to start at 1 this
        // will break.
        return taskId == partitionId;
    }

    /**
     * When a connector reconfiguration event is received this method should be called to ensure the correct strategy is
     * being implemented by the connector.
     *
     * @param maxTasks
     *            maximum number of configured tasks for this connector
     */
    @Override
    public void configureDistributionStrategy(final int maxTasks) {
        this.maxTasks = maxTasks;
    }
}
