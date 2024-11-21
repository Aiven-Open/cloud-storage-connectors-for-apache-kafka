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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionInPathDistributionStrategy allows source connectors to distribute tasks based off the folder structure that
 * partition number that is defined in that structure. for example /PREFIX/partition={{partition}}/YYYY/MM/DD/mm this
 * will split all tasks by the number of unique partitions defined in the storage path. e.g. Task distribution in
 * Connect with 10 Partitions and 3 tasks |Task | Partition| |0|0| |1|1| |2|2| |0|3| |1|4| |2|5| |0|6| |1|7| |2|8| |0|9|
 */
public final class PartitionInPathDistributionStrategy implements ObjectDistributionStrategy {
    public static final String PARTITION_ID_PATTERN = "\\{\\{partition}}";
    private final static Logger LOG = LoggerFactory.getLogger(PartitionInPathDistributionStrategy.class);

    private String prefix;
    private int maxTasks;

    PartitionInPathDistributionStrategy(final int maxTasks, final String expectedPathFormat) {
        configureDistributionStrategy(maxTasks, expectedPathFormat);
    }

    @Override
    public boolean isPartOfTask(final int taskId, final String pathToBeEvaluated) {
        if (pathToBeEvaluated == null || !pathToBeEvaluated.startsWith(prefix)) {
            LOG.warn("Ignoring path {}, does not contain the preconfigured prefix {} set up at startup",
                    pathToBeEvaluated, prefix);
            return false;
        }
        final String modifiedPath = StringUtils.substringAfter(pathToBeEvaluated, prefix);
        if (!modifiedPath.contains("/")) {
            LOG.warn("Ignoring path {}, does not contain any sub folders after partitionId prefix {}",
                    pathToBeEvaluated, prefix);
            return false;
        }
        final String partitionId = StringUtils.substringBefore(modifiedPath, "/");

        try {
            return toBeProcessedByThisTask(taskId, maxTasks, Integer.parseInt(partitionId));
        } catch (NumberFormatException ex) {
            throw new ConnectException(String
                    .format("Unexpected non integer value found parsing path for partitionId: %s", pathToBeEvaluated));
        }
    }

    /**
     *
     * @param maxTasks
     *            The maximum number of configured tasks for this
     * @param expectedPathFormat
     *            The format of the path and where to identify
     */

    @Override
    public void reconfigureDistributionStrategy(final int maxTasks, final String expectedPathFormat) {
        configureDistributionStrategy(maxTasks, expectedPathFormat);
    }

    private void configureDistributionStrategy(final int maxTasks, final String expectedPathFormat) {
        setMaxTasks(maxTasks);

        if (StringUtils.isEmpty(expectedPathFormat) || !expectedPathFormat.contains(PARTITION_ID_PATTERN)) {
            throw new ConfigException(String.format(
                    "Expected path format %s is missing the identifier '%s' to correctly select the partition",
                    expectedPathFormat, PARTITION_ID_PATTERN));
        }
        prefix = StringUtils.substringBefore(expectedPathFormat, PARTITION_ID_PATTERN);
    }

    private void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }

}
