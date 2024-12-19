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

package io.aiven.kafka.connect.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceTaskContext;

public class OffsetManager<E extends OffsetManager.OffsetManagerEntry<E>> {

    /**
     * The local manager data.
     */
    private final Map<Map<String, Object>, Map<String, Object>> offsets;

    /**
     * The context in which this is running.
     */
    private final SourceTaskContext context;

    /**
     * Constructor
     *
     * @param context
     *            the context for this instance to use.
     */
    public OffsetManager(final SourceTaskContext context) {
        this(context, new ConcurrentHashMap<>());
    }

    /**
     * Package private for testing.
     *
     * @param context
     *            the context for this instance to use.
     * @param offsets
     *            the offsets
     */
    protected OffsetManager(final SourceTaskContext context,
            final Map<Map<String, Object>, Map<String, Object>> offsets) {
        this.context = context;
        this.offsets = offsets;
    }

    /**
     * Get an entry from the offset manager. This method will return the local copy if it has been created otherwise
     * will get the data from Kafka.
     *
     * @param key
     *            the key for the entry.
     * @param creator
     *            a function to create the connector defined offset entry from a Map of string to object.
     * @return the entry.
     */
    public E getEntry(final OffsetManagerKey key, final Function<Map<String, Object>, E> creator) {
        final Map<String, Object> data = offsets.compute(key.getPartitionMap(), (k, v) -> {
            if (v == null) {
                final Map<String, Object> kafkaData = context.offsetStorageReader().offset(key.getPartitionMap());
                return kafkaData == null || kafkaData.isEmpty() ? new HashMap<>(key.getPartitionMap()) : kafkaData;
            } else {
                return v;
            }
        });
        return creator.apply(data);
    }

    /**
     * Copies the entry into the offset manager data.
     *
     * @param entry
     *            the entry to update.
     */
    public void updateCurrentOffsets(final E entry) {
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
    public interface OffsetManagerEntry<T extends OffsetManagerEntry<T>> extends Comparable<T> {

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

        /**
         * Gets the Kafka topic for this entry.
         * @return The Kafka topic for this entry.
         */
        String getTopic();

        /**
         * Gets the Kafka partition for this entry.
         * @return The Kafka partition for this entry.
         */
        Integer getPartition();

        /**
         * Gets the number of records to skip to get to this record.
         * This is the same as the zero-based index of this record if all records were in an array.
         * @return The number of records to skip to get to this record.
         */
        default long skipRecords() {
            return 0;
        }

        /**
         * Increments the record count.
         */
        void incrementRecordCount();
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
