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
 * {@link HashDistributionStrategy} evenly distributes cloud storage objects between tasks using the hashcode of the
 * object's filename, which is uniformly distributed and deterministic across workers.
 * <p>
 * This is well-suited to use cases where the order of events between records from objects is not important, especially
 * when ingesting files into Kafka that were not previously created by a supported cloud storage Sink.
 */
public final class HashDistributionStrategy extends DistributionStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(HashDistributionStrategy.class);
    public HashDistributionStrategy(final int maxTasks) {
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
        if (ctx.getStorageKey().isEmpty()) {
            LOG.warn("Ignoring as it is not passing a correct filename to be evaluated.");
            return UNDEFINED;
        }

        // floor mod returns the remainder of a division so will start at 0 and move up
        // tasks start at 0 so there should be no issue.
        return Math.floorMod(ctx.getStorageKey().hashCode(), maxTasks);
    }

}
