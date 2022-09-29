/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.compatibility.features;

/**
 * A {@link Feature} represents a part of the Kafka Connect API whose availability depends on teh version of the Kafka
 * Connect runtime that the connector (or converter, or transform, etc.) is deployed onto.
 */
public abstract class Feature {

    private Boolean supported;

    protected Feature() {
        supported = null;
    }

    /**
     * Check to see if the feature is supported. This method will be invoked at most once over the lifetime of this
     * instance; subclasses do not need to implement caching logic.
     * @return whether the feature is supported
     */
    protected abstract boolean checkSupported();

    /**
     * @return a human-readable name for the feature, such as "Sink task preCommit" or
     *     "Source task defined transactions"
     */
    public abstract String toString();

    /**
     * Determine whether the given feature is supported by the current Kafka Connect runtime
     * @return whether the feature is supported
     */
    public boolean supported() {
        if (supported != null) {
            return supported;
        }
        synchronized (this) {
            if (supported != null) {
                return supported;
            }
            return supported = checkSupported();
        }
    }

}
