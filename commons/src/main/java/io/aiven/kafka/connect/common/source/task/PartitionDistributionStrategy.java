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
public final class PartitionDistributionStrategy extends DistributionStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionDistributionStrategy.class);

    public PartitionDistributionStrategy(final int maxTasks) {
        super(maxTasks);
    }

    /**
     *
     * @param ctx
     *            is the Context which contains the storage key and optional values for the patition and topic
     * @return the task id this context should be assigned to or -1 if it is indeterminable
     */
    @Override
    public int getTaskFor(final Context<?> ctx) {
        final Optional<Integer> partitionId = ctx.getPartition();
        if (partitionId.isPresent()) {
            return partitionId.get() % maxTasks;
        }
        LOG.warn("Unable to find the partition from this file name {}", ctx.getStorageKey());
        return UNDEFINED;
    }
}
