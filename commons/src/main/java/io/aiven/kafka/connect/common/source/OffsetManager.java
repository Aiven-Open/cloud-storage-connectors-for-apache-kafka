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

package io.aiven.kafka.connect.common.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManager<E extends OffsetManager.OffsetManagerEntry<E>> {
    /** The loger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);

    /**
     * The local manager data.
     */
    private final ConcurrentMap<Map<String, Object>, Map<String, Object>> offsets;

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
            final ConcurrentMap<Map<String, Object>, Map<String, Object>> offsets) {
        this.context = context;
        this.offsets = offsets;
    }

    /**
     * Get an entry from the offset manager. This method will return the local copy if it has been created otherwise
     * will get the data from Kafka. If there is not a local copy and not one from Kafka then an empty Optional is
     * returned
     *
     * @param key
     *            the key for the entry.
     * @param creator
     *            a function to create the connector defined offset entry from a Map of string to object.
     * @return the entry.
     */
    public Optional<E> getEntry(final OffsetManagerKey key, final Function<Map<String, Object>, E> creator) {
        LOGGER.debug("getEntry: {}", key.getPartitionMap());
        final Map<String, Object> data = offsets.compute(key.getPartitionMap(), (k, v) -> {
            if (v == null) {
                final Map<String, Object> kafkaData = context.offsetStorageReader().offset(key.getPartitionMap());
                LOGGER.debug("Context stored offset map {}", kafkaData);
                return kafkaData == null || kafkaData.isEmpty() ? null : kafkaData;
            } else {
                LOGGER.debug("Previously stored offset map {}", v);
                return v;
            }
        });
        return data == null ? Optional.empty() : Optional.of(creator.apply(data));
    }

    /**
     * Copies the entry into the offset manager data.
     *
     * @param entry
     *            the entry to update.
     */
    public void updateCurrentOffsets(final E entry) {
        LOGGER.debug("Updating current offsets: {}", entry.getManagerKey().getPartitionMap());
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
     * Removes the specified entry from the in memory table. Does not impact the records stored in the
     * {@link SourceTaskContext}.
     *
     * @param key
     *            the key for the entry to remove.
     */
    public void remove(final OffsetManagerKey key) {
        LOGGER.debug("Removing: {}", key.getPartitionMap());
        offsets.remove(key.getPartitionMap());
    }

    /**
     * Removes the specified entry from the in memory table. Does not impact the records stored in the
     * {@link SourceTaskContext}.
     *
     * @param sourceRecord
     *            the SourceRecord that contains the key to be removed.
     */
    public void remove(final SourceRecord sourceRecord) {
        LOGGER.debug("Removing: {}", sourceRecord.sourcePartition());
        offsets.remove(sourceRecord.sourcePartition());
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
         * Gets the value of the named property as an {@code int}.
         *
         * @param key
         *            the property to retrieve.
         * @return the value associated with the property or @{code null} if not set.
         * @throws NullPointerException
         *             if a {@code null} key is not supported.
         */
        default int getInt(final String key) {
            return ((Number) getProperty(key)).intValue();
        }

        /**
         * Gets the value of the named property as a {@code long}
         *
         * @param key
         *            the property to retrieve.
         * @return the value associated with the property or @{code null} if not set.
         * @throws NullPointerException
         *             if a {@code null} key is not supported.
         */
        default long getLong(final String key) {
            return ((Number) getProperty(key)).longValue();
        }

        /**
         * Gets the value of the named property as a String.
         *
         * @param key
         *            the property to retrieve.
         * @return the value associated with the property or @{code null} if not set.
         * @throws NullPointerException
         *             if a {@code null} key is not supported.
         */
        default String getString(final String key) {
            return getProperty(key).toString();
        }

        /**
         * ManagerKey getManagerKey
         *
         * @return The offset manager key for this entry.
         */
        OffsetManagerKey getManagerKey();

        /**
         * Gets the Kafka topic for this entry.
         *
         * @return The Kafka topic for this entry.
         */
        String getTopic();

        /**
         * Gets the Kafka partition for this entry.
         *
         * @return The Kafka partition for this entry.
         */
        int getPartition();

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
