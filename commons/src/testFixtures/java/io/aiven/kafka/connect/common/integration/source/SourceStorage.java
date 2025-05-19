/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.common.integration.source;

import io.aiven.kafka.connect.common.integration.StorageBase;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.source.OffsetManager;

/**
 *
 *
 * @param <K>
 *            the native key type
 * @param <N>
 *            The native object type.
 * @param <O>
 *            The OffsetManagerEntry type.
 * @param <T>
 *            The concrete implementation of the {@link AbstractSourceRecord} .
 */
public interface SourceStorage<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends SourceStorage<K, N, O, T>>
        extends
            StorageBase<K, N> {

    /**
     * Convert a string into the key value for the native object. In most cases the underlying system uses a string so
     * returning the {@code key} argument is appropriate. However, this method provides an opportunity to convert the
     * key into something that the native system would produce.
     *
     * @param key
     *            the key value as a string.
     * @return the native key equivalent of the {@code key} parameter.
     */
    K createKFrom(String key);

    /**
     * Create an offset manager entry from the string key value,
     *
     * @param key
     *            the key value as a string.
     * @return an OffsetManager entry.
     */
    O createOffsetManagerEntry(String key);

    /**
     * Creates the source record under test.
     *
     * @return the source record under test.
     */
    T createSourceRecord();
}
