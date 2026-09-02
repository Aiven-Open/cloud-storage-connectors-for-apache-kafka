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

import java.util.Map;
import java.util.function.BiFunction;

import io.aiven.kafka.connect.common.integration.StorageBase;
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
 */
public interface SourceStorage<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>>
        extends
            StorageBase<K, N> {

    K createKey(String prefix, String topic, int partition);

    WriteResult<K> writeWithKey(K nativeKey, byte[] testDataBytes);

    Map<String, String> createConnectorConfig(String localPrefix);

    BiFunction<Map<String, Object>, Map<String, Object>, O> offsetManagerEntryFactory();

    /**
     * The result of a successful write.
     *
     * @param <K>
     *            the native key type.
     */
    final class WriteResult<K extends Comparable<K>> {
        /** the OffsetManagerKey for the result */
        private final OffsetManager.OffsetManagerKey offsetKey;
        /** The native Key for the result */
        private final K nativeKey;

        /**
         * Constructor.
         *
         * @param offsetKey
         *            the OffsetManagerKey for the result.
         * @param nativeKey
         *            the native key for the result.
         */
        public WriteResult(final OffsetManager.OffsetManagerKey offsetKey, final K nativeKey) {
            this.offsetKey = offsetKey;
            this.nativeKey = nativeKey;
        }

        /**
         * Gets the OffsetManagerKey.
         *
         * @return the OffsetManagerKey
         */
        public OffsetManager.OffsetManagerKey getOffsetManagerKey() {
            return offsetKey;
        }

        /**
         * Gets the native key.
         *
         * @return the native key.
         */
        K getNativeKey() {
            return nativeKey;
        }
    }
}
