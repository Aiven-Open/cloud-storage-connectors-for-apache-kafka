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
import java.util.function.Function;

/**
 * An {@link DistributionStrategy} provides a mechanism to share the work of processing records from objects (or files)
 * into tasks, which are subsequently processed (potentially in parallel) by Kafka Connect workers.
 * <p>
 * The number of objects in cloud storage can be very high, selecting a distribution strategy allows the connector to
 * know how to distribute the load across Connector tasks and in some cases using an appropriate strategy can also
 * decide on maintaining a level of ordering between messages as well.
 */
public final class DistributionStrategy {
    private int maxTasks;
    private final Function<Context<?>, Optional<Long>> mutation;
    public final static int UNDEFINED = -1;

    public DistributionStrategy(final Function<Context<?>, Optional<Long>> creator, final int maxTasks) {
        assertPositiveInteger(maxTasks);
        this.mutation = creator;
        this.maxTasks = maxTasks;
    }

    private static void assertPositiveInteger(final int sourceInt) {
        if (sourceInt <= 0) {
            throw new IllegalArgumentException("tasks.max must be set to a positive number and at least 1.");
        }
    }

    /**
     * Retrieve the taskId that this object should be processed by. Any single object will be assigned deterministically
     * to a single taskId, that will be always return the same taskId output given the same context is used.
     *
     * @param ctx
     *            This is the context which contains optional values for the partition, topic and storage key name
     * @return the taskId which this particular task should be assigned to.
     */
    public int getTaskFor(final Context<?> ctx) {
        return mutation.apply(ctx).map(aLong -> Math.floorMod(aLong, maxTasks)).orElse(UNDEFINED);
    }

    /**
     * When a connector receives a reconfigure event this method should be called to ensure that the distribution
     * strategy is updated correctly.
     *
     * @param maxTasks
     *            The maximum number of tasks created for the Connector
     */
    public void configureDistributionStrategy(final int maxTasks) {
        assertPositiveInteger(maxTasks);
        this.maxTasks = maxTasks;
    }
}
