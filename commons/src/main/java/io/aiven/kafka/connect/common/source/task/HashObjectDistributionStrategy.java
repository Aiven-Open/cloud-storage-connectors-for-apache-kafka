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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HashTaskAssignmentStrategy determines which files should be executed by a task by the filename and path supplied by
 * the iterator. The RandomTaskAssignment is perfect in use cases where ordering of events is not a requirement and when
 * adding files to kafka where the files were not previously created by a supported S3 Sink or where manually created or
 * created by another process.
 */
public final class HashObjectDistributionStrategy implements ObjectDistributionStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(HashObjectDistributionStrategy.class);
    private int maxTasks;
    HashObjectDistributionStrategy(final int maxTasks) {
        this.maxTasks = maxTasks;
    }

    @Override
    public boolean isPartOfTask(final int taskId, final String filenameToBeEvaluated) {
        if (filenameToBeEvaluated == null) {
            LOG.warn("Ignoring as it is not passing a correct filename to be evaluated.");
            return false;
        }
        final int taskAssignment = Math.floorMod(filenameToBeEvaluated.hashCode(), maxTasks);
        // floor mod returns the remainder of a division so will start at 0 and move up
        // tasks start at 0 so there should be no issue.
        return taskAssignment == taskId;
    }

    @Override
    public void reconfigureDistributionStrategy(final int maxTasks, final String expectedFormat) {
        setMaxTasks(maxTasks);
    }

    public void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }
}
