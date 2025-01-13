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

import java.util.regex.Pattern;

/**
 * An {@link DistributionStrategy} provides a mechanism to share the work of processing records from objects (or files)
 * into tasks, which are subsequently processed (potentially in parallel) by Kafka Connect workers.
 * <p>
 * The number of objects in cloud storage can be very high, and they are distributed amongst tasks to minimize the
 * overhead of assigning work to Kafka worker threads. All objects assigned to the same task will be processed together
 * sequentially by the same worker, which can be useful for maintaining order between objects. There are usually fewer
 * workers than tasks, and they will be assigned the remaining tasks as work completes.
 */
public interface DistributionStrategy {
    /**
     * Check if the object should be processed by the task with the given {@code taskId}. Any single object should be
     * assigned deterministically to a single taskId.
     *
     * @param taskId
     *            a task ID, usually for the currently running task
     * @param valueToBeEvaluated
     *            The value to be evaluated to determine if it should be processed by the task.
     * @return true if the task should process the object, false if it should not.
     */
    boolean isPartOfTask(int taskId, String valueToBeEvaluated, Pattern filePattern);

    /**
     * When a connector receives a reconfigure event this method should be called to ensure that the distribution
     * strategy is updated correctly.
     *
     * @param maxTasks
     *            The maximum number of tasks created for the Connector
     */
    void configureDistributionStrategy(int maxTasks);
}
