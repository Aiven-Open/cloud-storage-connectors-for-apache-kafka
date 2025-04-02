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

package io.aiven.kafka.connect.common.source;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public final class OffsetManager<E extends OffsetManager.OffsetManagerEntry<E>> {
    /** The logger to write to */
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
        this.context = context;
        this.offsets = new ConcurrentHashMap<>();
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
     * Get add an entry to the offset manager. For retrieval later
     *
     * @param entry
     *            the entry that should be added to the offset manager.
     *
     */
    public void addEntry(final OffsetManagerEntry<E> entry) {
        offsets.put(entry.getManagerKey().getPartitionMap(), entry.getProperties());
    }

    /**
     * Gets any offset information stored in the offsetStorageReader and adds to the local offsets Map. This provides a
     * performance improvement over when checking if offsets exists individually.
     *
     * @param offsetManagerKeys
     *            A Collection of OffsetManagerKey which identify individual offset entries
     */
    public void populateOffsetManager(final Collection<OffsetManager.OffsetManagerKey> offsetManagerKeys) {
        final Map<Map<String, Object>, Map<String, Object>> matchingOffsets = new HashMap<>();
        context.offsetStorageReader()
                .offsets(offsetManagerKeys.stream().map(OffsetManagerKey::getPartitionMap).collect(toList()))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> matchingOffsets.put(entry.getKey(), entry.getValue()));
        // offsets is multithreaded batching in matchingOffsets is faster
        offsets.putAll(matchingOffsets);
    }

    /**
     * Removes the specified entry from the in memory table. Does not impact the records stored in the
     * {@link SourceTaskContext}.
     *
     * @param key
     *            the key for the entry to remove.
     */
    public void removeEntry(final OffsetManagerKey key) {
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
    public void removeEntry(final SourceRecord sourceRecord) {
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
         * <p>
         * This method should make a copy of the internal data and return that to prevent any accidental updates to the
         * internal data.
         * </p>
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
         * Gets the OffsetManagerKey for this entry.
         * <p>
         * The return value should be a copy of the internal structure or constructed in such a way that modification to
         * the key values is not reflected in the OffsetManagerEntry.
         * </p>
         *
         * @return The offset manager key for this entry.
         */
        OffsetManagerKey getManagerKey();

        /**
         * Increments the record count.
         */
        void incrementRecordCount();

        /**
         * Gets the current record count.
         *
         * @return The current record count.
         */
        long getRecordCount();
    }

    /**
     * The OffsetManager Key. Must override hashCode() and equals().
     */
    public static final class OffsetManagerKey {
        private final SortedMap<String, Object> data;
        private final int hashCode;

        public OffsetManagerKey(Map<String, Object> data) {
            this.data = new TreeMap<>(data);
            HashCodeBuilder builder = new HashCodeBuilder();
            for (final Object value : this.data.values()) {
                builder.append(value);
            }
            hashCode = builder.toHashCode();
        }

        public Map<String, Object> getPartitionMap() {
            return data;
        }

        @Override
        public String toString() {
            return data.toString();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            EqualsBuilder builder = new EqualsBuilder();
            if (other instanceof OffsetManager.OffsetManagerKey) {
                Map<String, Object> lhs = data;
                Map<String, Object> rhs = ((OffsetManager.OffsetManagerKey) other).data;
                if (lhs.size() != rhs.size()) {
                    return false;
                }
                Iterator<Map.Entry<String, Object>> lhsIterator = lhs.entrySet().iterator();
                Iterator<Map.Entry<String, Object>> rhsIterator = rhs.entrySet().iterator();
                while (lhsIterator.hasNext() && rhsIterator.hasNext()) {
                    Map.Entry<String, Object> lhsEntry = lhsIterator.next();
                    Map.Entry<String, Object> rhsEntry = rhsIterator.next();
                    if (!(lhsEntry.getKey().equals(rhsEntry.getKey()) && lhsEntry.getValue().equals(rhsEntry.getValue()))) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }
}
