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

package io.aiven.kafka.connect.common.source.offsets;

import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);
    public static final String SEPARATOR = "_";
    // TODO look using map maker to eject keys which are no longer needed. (i.e. there is only a requirement to actually
    // have the last file completed in memory plus the current file)
    // Guava's CacheBuilder might be a good choice
    private final Map<Map<String, Object>, Map<String, Object>> offsets;

    public OffsetManager(final SourceTaskContext context, final List<Map<String, Object>> partitionKeys) {

        this.offsets = context.offsetStorageReader()
                .offsets(partitionKeys)
                .entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(toMap(entry -> new HashMap<>(entry.getKey()), entry -> new HashMap<>(entry.getValue())));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(" ********** offsets ***** {}", offsets);
        }
    }

    /**
     * FOR TESTING ONLY
     *
     * @param offsets
     *            the offsets
     */
    protected OffsetManager(final Map<Map<String, Object>, Map<String, Object>> offsets) {
        this.offsets = offsets;
    }

    /**
     * @deprecated use getEntry() instead.
     * @return an unmodifiable map of offsets
     */
    @Deprecated
    public Map<Map<String, Object>, Map<String, Object>> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    public <T extends OffsetManagerEntry> T getEntry(final OffsetManagerKey key,
            final Function<Map<String, Object>, T> creator) {
        return creator.apply(offsets.get(key.getPartitionMap()));
    }

    public void updateCurrentOffsets(final OffsetManagerEntry entry) {
        offsets.compute(entry.getManagerKey().getPartitionMap(), (k, v) -> {
            if (v == null) {
                return new HashMap<>(entry.getProperties());
            } else {
                v.putAll(entry.getProperties());
                return v;
            }
        });
    }

    /**
     * The definition of an entry in the OffsetManager.
     */
    public interface OffsetManagerEntry<T extends OffsetManagerEntry> extends Comparable<T> {

        /**
         * Creates a new OffsetManagerEntry by wrapping the properties with the current implementation. This method may
         * throw a RuntimeException if requried properties are not defined in the map.
         *
         * @param properties
         *            the properties to wrap. May be {@code null}.
         * @return an OffsetManagerProperty
         */
        T fromProperties(Map<String, Object> properties);

        /**
         * Extracts the data from the entry in the correct format to return to Kafka.
         *
         * @return the properties in a format to return to Kafka.
         */
        Map<String, Object> getProperties();

        /**
         * Gets the value of the named property. The value returned from a {@code null} key is implementation dependant.
         *
         * @param key
         *            the property to retrieve.
         * @return the value associated with the property or @{code null} if not set.
         * @throws NullPointerException
         *             if a {@code null} key is not supported.
         */
        Object getProperty(String key);

        /**
         * Sets a key/value pair. Will overwrite any existing value. Implementations of OffsetManagerEntry may declare
         * specific keys as restricted. These are generally keys that are managed internally by the OffsetManagerEntry
         * and may not be set except through provided setter methods or the constructor.
         *
         * @param key
         *            the key to set.
         * @param value
         *            the value to set.
         * @throws IllegalArgumentException
         *             if the key is restricted.
         */
        void setProperty(String key, Object value);

        /**
         * ManagerKey getManagerKey
         *
         * @return The offset manager key for this entry.
         */
        OffsetManagerKey getManagerKey();
    }

    /**
     * The OffsetManager Key. Must override hashCode() and equals().
     */
    @FunctionalInterface
    public interface OffsetManagerKey {
        /**
         * gets the partition map used by Kafka to identify this Offset entry.
         *
         * @return The partition map used by Kafka to identify this Offset entry.
         */
        Map<String, Object> getPartitionMap();
    }
}
