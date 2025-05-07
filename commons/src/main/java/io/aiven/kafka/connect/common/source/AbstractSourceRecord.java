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

import io.aiven.kafka.connect.common.storage.NativeInfo;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.Context;

import org.slf4j.Logger;

/**
 * An abstract source record as retrieved from the storage layer.
 *
 * @param <N>
 *            the native object type.
 * @param <K>
 *            the key type for the native object.
 * @param <O>
 *            the OffsetManagerEntry for the iterator.
 * @param <E>
 *            the implementation class for AbstractSourceRecord.
 */
public abstract class AbstractSourceRecord<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, E extends AbstractSourceRecord<N, K, O, E>> {
    /** The logger from the implementation class */
    private final Logger logger;
    /** the key for the source record */
    private SchemaAndValue keyData;
    /** The value for the source record. */
    private SchemaAndValue valueData;
    /** The offset manager entry for this record */
    private O offsetManagerEntry;
    /** The context associated with this record */
    private Context<K> context;
    /** The native info for this record. */
    private final NativeInfo<N, K> nativeInfo;

    /**
     * Construct a source record from a native item.
     *
     * @param logger
     *            The logger for the implementation class.
     * @param nativeInfo
     *            the native information for the native that his record represents.
     */
    public AbstractSourceRecord(final Logger logger, final NativeInfo<N, K> nativeInfo) {
        this.logger = logger;
        this.nativeInfo = nativeInfo;
    }

    /**
     * Copy constructor for an abstract source record. This constructor makes a copy of the OffsetManagerEntry.
     *
     * @param sourceRecord
     *            the source record to copy.
     */
    protected AbstractSourceRecord(final AbstractSourceRecord<N, K, O, E> sourceRecord) {
        this(sourceRecord.logger, sourceRecord.nativeInfo);
        this.offsetManagerEntry = sourceRecord.offsetManagerEntry
                .fromProperties(sourceRecord.getOffsetManagerEntry().getProperties());
        this.keyData = sourceRecord.keyData;
        this.valueData = sourceRecord.valueData;
        this.context = sourceRecord.context;
    }

    /**
     * Gets then key for the native object.
     *
     * @return The key for the native object.
     */
    final public K getNativeKey() {
        return nativeInfo.getNativeKey();
    }

    /**
     * Gets the number of bytes in the input stream extracted from the native object.
     *
     * @return The number of bytes in the input stream extracted from the native object.
     */
    final public long getNativeItemSize() {
        return nativeInfo.getNativeItemSize();
    }

    /**
     * Makes a duplicate of this AbstractSourceRecord. This is similar to the Java {@code clone} method but without the
     * baggage.
     *
     * @return A duplicate of this AbstractSourceRecord
     */
    abstract public E duplicate();

    /**
     * Gets the native item that this source record is working with.
     *
     * @return The native item that this source record is working with.
     */
    final public N getNativeItem() {
        return nativeInfo.getNativeItem();
    }

    /**
     * Gets the record count as recorded by the OffsetManager for the native item that this source record is working
     * with.
     *
     * @return The record count as recorded by the OffsetManager for the native item that this source record is working
     *         with.
     */
    final public long getRecordCount() {
        return offsetManagerEntry == null ? 0 : offsetManagerEntry.getRecordCount();
    }

    /**
     * Increments the record count as recorded by the OffsetManager for the native item that this source record is
     * working with.
     */
    final public void incrementRecordCount() {
        this.offsetManagerEntry.incrementRecordCount();
    }

    /**
     * Sets the key data for this source record.
     *
     * @param keyData
     *            The key data for this source record.
     */
    final public void setKeyData(final SchemaAndValue keyData) {
        this.keyData = keyData;
    }

    /**
     * Gets the key data for this source record. Makes a defensive copy.
     *
     * @return A copy of the key data for this source record.
     */
    final public SchemaAndValue getKey() {
        return new SchemaAndValue(keyData.schema(), keyData.value());
    }

    /**
     * Sets the value data for this source record.
     *
     * @param valueData
     *            The key data for this source record.
     */
    final public void setValueData(final SchemaAndValue valueData) {
        this.valueData = valueData;
    }

    /**
     * Gets the value data for this source record. Makes a defensive copy.
     *
     * @return A copy of the key data for this source record.
     */
    final public SchemaAndValue getValue() {
        return new SchemaAndValue(valueData.schema(), valueData.value());
    }

    /**
     * Gets the topic for the source record.
     *
     * @return The topic for the source record or {@code null} if it is not set in the context.
     */
    final public String getTopic() {
        return context.getTopic().orElse(null);
    }

    /**
     * Gets the partition for the source record.
     *
     * @return The partition for the source record or {@code null} if it is not set in the context.
     */
    final public Integer getPartition() {
        return context.getPartition().orElse(null);
    }

    /**
     * Sets the offset manager entry for this source record.
     *
     * @param offsetManagerEntry
     *            The offset manager entry for this source record.
     */
    final public void setOffsetManagerEntry(final O offsetManagerEntry) {
        this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
    }

    /**
     * Gets the offset manager entry for this source record. Makes a defensive copy.
     *
     * @return A copy of the offset manager entry for this source record.
     */
    final public O getOffsetManagerEntry() {
        return offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties()); // return a defensive copy
    }

    /**
     * Gets the Context for this source record. Makes a defensive copy.
     *
     * @return A copy of the Context for this source record.
     */
    final public Context<K> getContext() {
        return new Context<>(context) {
        };
    }

    /**
     * Sets the Context for this source record. Makes a defensive copy.
     *
     * @param context
     *            The Context for this source record.
     */
    final public void setContext(final Context<K> context) {
        this.context = new Context<>(context) {
        };
    }

    /**
     * Creates a SourceRecord that can be returned to a Kafka topic
     *
     * @param tolerance
     *            The error tolerance for the record processing.
     * @param offsetManager
     *            The offset manager for the offset entry.
     * @return A kafka {@link SourceRecord SourceRecord} This can return null if error tolerance is set to 'All'
     */
    final public SourceRecord getSourceRecord(final ErrorsTolerance tolerance, final OffsetManager<O> offsetManager) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Source Record: {} for Topic: {} , Partition: {}, recordCount: {}", getNativeKey(),
                        getTopic(), getPartition(), getRecordCount());
            }
            offsetManager.addEntry(offsetManagerEntry);
            return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                    offsetManagerEntry.getProperties(), getTopic(), getPartition(), keyData.schema(), keyData.value(),
                    valueData.schema(), valueData.value());
        } catch (DataException e) {
            if (ErrorsTolerance.NONE.equals(tolerance)) {
                throw new ConnectException("Data Exception caught during record to source record transformation", e);
            } else {
                logger.warn(
                        "Data Exception caught during record to source record transformation {} . errors.tolerance set to 'all', logging warning and continuing to process.",
                        e.getMessage(), e);
                return null;
            }
        }
    }

}
