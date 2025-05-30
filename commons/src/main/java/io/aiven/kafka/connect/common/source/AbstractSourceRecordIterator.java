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

import javax.validation.constraints.NotNull;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;

/**
 * Iterator that processes cloud storage items and creates Kafka source records. Supports multiple output formats.
 *
 * @param <N>
 *            the native object type.
 * @param <K>
 *            the key type for the native object.
 * @param <O>
 *            the OffsetManagerEntry for the iterator.
 * @param <T>
 *            The source record for the client type.
 *
 */
public abstract class AbstractSourceRecordIterator<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>>
        implements
            Iterator<T> {
    /** The OffsetManager that we are using */
    private final OffsetManager<O> offsetManager;

    /** The configuration for the source */
    private final SourceCommonConfig sourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** the taskId of this running task */
    private final int taskId;

    /**
     * The inner iterator to provides a base AbstractSourceRecord for a storage item that has passed the filters and
     * potentially had data extracted.
     */
    private Iterator<T> inner;
    /**
     * The outer iterator that provides an AbstractSourceRecord for each record contained by the storage item identified
     * by the inner record.
     */
    private Iterator<T> outer;
    /** The topic(s) which have been configured with the 'topics' configuration */
    private final Optional<String> targetTopics;
    /** Check if the native item key is part of the 'target' files configured to be extracted from Azure */
    protected final FileMatching fileMatching;
    /** The predicate which will determine if an native item should be assigned to this task for processing */
    protected final Predicate<Optional<T>> taskAssignment;
    /**
     * The native item key which is currently being processed, when rehydrating from the storage engine we will skip
     * other native items that come before this key.
     */
    private K lastSeenNativeKey;
    /**
     * The ring buffer which contains recently processed native item keys, this is used during a restart to skip keys
     * that are known to have been processed while still accounting for the possibility that slower writing to storage
     * may have introduced newer keys.
     */
    private final RingBuffer<K> ringBuffer;

    /**
     * Constructor.
     *
     * @param sourceConfig
     *            The source configuration.
     * @param offsetManager
     *            the Offset manager to use.
     * @param transformer
     *            the transformer to use.
     * @param bufferSize
     *            the size of the ring buffer.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public AbstractSourceRecordIterator(final SourceCommonConfig sourceConfig, final OffsetManager<O> offsetManager,
            final Transformer transformer, final int bufferSize) {
        super();

        final DistributionType distributionType = sourceConfig.getDistributionType();
        final int maxTasks = sourceConfig.getMaxTasks();

        this.sourceConfig = sourceConfig;
        this.offsetManager = offsetManager;
        this.transformer = transformer;
        this.targetTopics = Optional.ofNullable(sourceConfig.getTargetTopic());
        this.taskId = sourceConfig.getTaskId() % maxTasks;
        this.taskAssignment = new TaskAssignment(distributionType.getDistributionStrategy(maxTasks));
        this.fileMatching = new FileMatching(new FilePatternUtils(sourceConfig.getSourcename()));
        this.inner = Collections.emptyIterator();
        this.outer = Collections.emptyIterator();
        this.ringBuffer = new RingBuffer<>(Math.max(1, bufferSize));
    }

    /**
     * Gets the logger for the concrete implementation.
     *
     * @return The logger for the concrete implementation.
     */
    abstract protected Logger getLogger();

    /**
     * Get a stream of Native object from the underlying storage layer.
     *
     * @param offset
     *            the native key to start from. May be {@code null}.
     * @return A stream of natvie objects. May be empty but not {@code null}.
     */
    abstract protected Stream<N> getNativeItemStream(K offset);

    /**
     * Gets an IOSupplier for the specific source record.
     *
     * @param sourceRecord
     *            the source record to get the input stream from.
     * @return the IOSupplier that retrieves an InputStream from the source record.
     */
    abstract protected IOSupplier<InputStream> getInputStream(T sourceRecord);

    /**
     * Gets the native key for the native object.
     *
     * @param nativeObject
     *            the native object ot retrieve the native key for.
     * @return The native key for the native object.
     */
    abstract protected K getNativeKey(N nativeObject);

    /**
     * Gets the AbstractSourceRecord implementation for the native object.
     *
     * @param nativeObject
     *            the native object to get the AbstractSourceRecord for.
     * @return the AbstractSourceRecord for the natvie object.
     */
    abstract protected T createSourceRecord(N nativeObject);

    /**
     * Creates an OffsetManagerEntry for a native object.
     *
     * @param nativeObject
     *            the native object to create the OffsetManagerEntry for.
     * @return An OffsetManagerEntry for a native object.
     */
    abstract protected O createOffsetManagerEntry(N nativeObject);

    /**
     * Creates an offset manager key for the native key.
     *
     * @param nativeKey
     *            THe native key to create an offset manager key for.
     * @return An offset manager key.
     */
    abstract protected OffsetManager.OffsetManagerKey getOffsetManagerKey(@NotNull K nativeKey);

    @Override
    final public boolean hasNext() {
        if (!outer.hasNext() && lastSeenNativeKey != null) {
            // update the buffer to contain this new objectKey
            ringBuffer.enqueue(lastSeenNativeKey);
            // Remove the last seen from the offsetmanager as the file has been completely processed.
            offsetManager.removeEntry(getOffsetManagerKey(lastSeenNativeKey));
        }
        if (!inner.hasNext() && !outer.hasNext()) {
            inner = getNativeItemStream(ringBuffer.getOldest()).map(fileMatching)
                    .filter(taskAssignment)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .iterator();
        }
        while (!outer.hasNext() && inner.hasNext()) {
            outer = convert(inner.next()).iterator();
        }
        return outer.hasNext();
    }

    @Override
    final public T next() {
        return outer.next();
    }

    @Override
    final public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * Converts the native item into stream of AbstractSourceRecords.
     *
     * @param sourceRecord
     *            the SourceRecord that drives the creation of source records with values.
     * @return a stream of T created from the input stream of the native item.
     */
    private Stream<T> convert(final T sourceRecord) {
        sourceRecord
                .setKeyData(transformer.getKeyData(sourceRecord.getNativeKey(), sourceRecord.getTopic(), sourceConfig));

        lastSeenNativeKey = sourceRecord.getNativeKey();

        return transformer
                .getRecords(getInputStream(sourceRecord), sourceRecord.getNativeItemSize(), sourceRecord.getContext(),
                        sourceConfig, sourceRecord.getRecordCount())
                .map(new Mapper<N, K, O, T>(sourceRecord));

    }

    /**
     * Maps the data from the @{link Transformer} stream to an AbstractSourceRecord given all the additional data
     * required.
     *
     * @param <N>
     *            the native object type.
     * @param <K>
     *            the key type for the native object.
     * @param <O>
     *            the OffsetManagerEntry for the iterator.
     * @param <T>
     *            The source record for the client type.
     */
    static class Mapper<N, K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>>
            implements
                Function<SchemaAndValue, T> {
        /**
         * The AbstractSourceRecord that produces the values.
         */
        private final T sourceRecord;

        /**
         * Constructor.
         *
         * @param sourceRecord
         *            The source record to provide default values..
         */
        public Mapper(final T sourceRecord) {
            // operation within the Transformer
            // to see if there are more records.
            this.sourceRecord = sourceRecord;
        }

        @Override
        public T apply(final SchemaAndValue valueData) {
            sourceRecord.incrementRecordCount();
            final T result = sourceRecord.duplicate();
            result.setValueData(valueData);
            return result;
        }
    }

    /**
     * Determines if an AbstractSourceRecord belongs to this task.
     */
    class TaskAssignment implements Predicate<Optional<T>> {
        private final DistributionStrategy distributionStrategy;
        /**
         * Constructs a task assignment from the distribution strategy.
         *
         * @param distributionStrategy
         *            The distribution strategy.
         */
        TaskAssignment(final DistributionStrategy distributionStrategy) {
            this.distributionStrategy = distributionStrategy;
        }

        @Override
        public boolean test(final Optional<T> sourceRecord) {
            return sourceRecord.filter(t -> taskId == distributionStrategy.getTaskFor(t.getContext())).isPresent();
        }
    }

    /**
     * Attempts to match the name of the native item and extract the Context from it.
     */
    class FileMatching implements Function<N, Optional<T>> {
        /**
         * The file pattern utils that this file matching uses.
         */
        private final FilePatternUtils utils;

        /**
         * Created a FileMatching from the specified FilePatternUtils.
         *
         * @param utils
         *            the file pattern utils to use.
         */
        FileMatching(final FilePatternUtils utils) {
            this.utils = utils;
        }

        @Override
        public Optional<T> apply(final N nativeItem) {
            final K itemName = getNativeKey(nativeItem);
            final Optional<Context<K>> optionalContext = utils.process(itemName);
            if (optionalContext.isPresent() && !ringBuffer.contains(itemName)) {
                final T sourceRecord = createSourceRecord(nativeItem);
                final Context<K> context = optionalContext.get();
                overrideContextTopic(context);
                sourceRecord.setContext(context);
                O offsetManagerEntry = createOffsetManagerEntry(nativeItem);
                offsetManagerEntry = offsetManager
                        .getEntry(offsetManagerEntry.getManagerKey(), offsetManagerEntry::fromProperties)
                        .orElse(offsetManagerEntry);
                sourceRecord.setOffsetManagerEntry(offsetManagerEntry);
                return Optional.of(sourceRecord);
            }
            return Optional.empty();
        }

        /**
         * Sets the target topic in the context if it has been set from configuration.
         *
         * @param context
         *            the context to set the topic in if found.
         */
        private void overrideContextTopic(final Context<K> context) {
            if (targetTopics.isPresent()) {
                if (context.getTopic().isPresent()) {
                    getLogger().debug(
                            "Overriding topic '{}' extracted from native item name with topic '{}' from configuration 'topics'. ",
                            context.getTopic().get(), targetTopics.get());
                }
                context.setTopic(targetTopics.get());
            }
        }
    }
}
