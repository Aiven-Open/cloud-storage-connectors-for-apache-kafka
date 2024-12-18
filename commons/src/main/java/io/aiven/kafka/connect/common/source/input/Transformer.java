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

package io.aiven.kafka.connect.common.source.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.OffsetManager;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;

/**
 * The abstract base class for a Transformer. This class handles opening and closing the input stream. Implementations
 * of this class must implement a {@link StreamSpliterator}. The Stream Spliterator focuses on reading one record from
 * the inputstream.
 */
public abstract class Transformer {

    /**
     * Creates a stream of SchemaAndValue objects from the input stream. The input stream will be closed when the last
     * object is read from the stream. Will skip the number of records specified by the
     * {@link OffsetManager.OffsetManagerEntry#skipRecords()} value.
     *
     * @param inputStreamIOSupplier
     *            the supplier of an input stream.
     * @param offsetManagerEntry
     *            the offsetManagerEntry to use.
     * @param config
     *            the Abstract Config to use.
     * @return a Stream of SchemaAndValue objects.
     */
    public final Stream<SchemaAndValue> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier,
            final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, final AbstractConfig config) {
        final StreamSpliterator spliterator = createSpliterator(inputStreamIOSupplier, offsetManagerEntry, config);
        return StreamSupport.stream(spliterator, false)
                .onClose(spliterator::close)
                .skip(offsetManagerEntry.skipRecords());
    }

    /**
     * Get the schema to use for the key.
     *
     * @return the Schema to use for the key.
     */
    public abstract Schema getKeySchema();

    /**
     * Creates the stream spliterator for this transformer.
     *
     * @param inputStreamIOSupplier
     *            the input stream supplier.
     * @param offsetManagerEntry
     *            the offsetManagerEntry to use.
     * @param sourceConfig
     *            the source configuraiton.
     * @return a StreamSpliterator instance.
     */
    protected abstract StreamSpliterator createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier,
            OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, AbstractConfig sourceConfig);

    /**
     * A Spliterator that performs various checks on the opening/closing of the input stream.
     */
    protected abstract static class StreamSpliterator implements Spliterator<SchemaAndValue> {
        /**
         * The input stream supplier.
         */
        private final IOSupplier<InputStream> inputStreamIOSupplier;
        /**
         * The logger to be used by all instances of this class. This will be the Transformer logger.
         */
        protected final Logger logger;
        /**
         * The input stream. Will be null until {@link #inputOpened} has completed. May be used for reading but should
         * not be closed or otherwise made unreadable.
         */
        protected InputStream inputStream;
        /**
         * The OffsetManager to use for record tracking if necessary.
         */
        protected final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry;
        /**
         * A flag indicate that the input stream has been closed.
         */
        private boolean closed;

        /**
         * Constructor.
         *
         * @param logger
         *            The logger for this Spliterator to use.
         * @param offsetManagerEntry
         *            the offsetManagerEntry to use.
         * @param inputStreamIOSupplier
         *            the InputStream supplier
         */
        protected StreamSpliterator(final Logger logger, final IOSupplier<InputStream> inputStreamIOSupplier,
                final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry) {
            this.logger = logger;
            this.inputStreamIOSupplier = inputStreamIOSupplier;
            this.offsetManagerEntry = offsetManagerEntry;
        }

        /**
         * Attempt to read the next record. If there is no record to read or an error occurred return false. If a record
         * was created, call {@code action.accept()} with the record.
         *
         * @param action
         *            the Consumer to call if record is created.
         * @return {@code true} if a record was processed, {@code false} otherwise.
         */
        abstract protected boolean doAdvance(Consumer<? super SchemaAndValue> action);

        /**
         * Method to close additional inputs if needed. This method is called during the {@link #close()} method before
         * the input stream is closed.
         */
        abstract protected void doClose();

        /**
         * Closes the Spliterator which closes the input stream.
         */
        public final void close() {
            doClose();
            try {
                if (inputStream != null) {
                    inputStream.close();
                    closed = true;
                }
            } catch (IOException e) {
                logger.error("Error trying to close inputStream: {}", e.getMessage(), e);
            }
        }

        /**
         * Allows modification of input stream. Called immediately after the input stream is opened. Implementations may
         * modify the type of input stream by wrapping it with a specific implementation, or may create Readers from the
         * input stream. The modified input stream must be returned. If a Reader or similar class is created from the
         * input stream the input stream must be returned.
         *
         * @param input
         *            the input stream that was just opened.
         * @return the input stream or modified input stream.
         * @throws IOException
         *             on IO error.
         */
        abstract protected InputStream inputOpened(InputStream input) throws IOException;

        @Override
        public final boolean tryAdvance(final Consumer<? super SchemaAndValue> action) {
            boolean result = false;
            if (closed) {
                logger.error("Attempt to advance after closed");
            }
            try {
                if (inputStream == null) {
                    try {
                        inputStream = inputOpened(inputStreamIOSupplier.get());
                    } catch (IOException e) {
                        logger.error("Error trying to open inputStream: {}", e.getMessage(), e);
                        close();
                        return false;
                    }
                }
                result = doAdvance(action);
                if (result) {
                    offsetManagerEntry.incrementRecordCount();
                }
            } catch (RuntimeException e) { // NOPMD must catch runtime exception here.
                logger.error("Error trying to advance data: {}", e.getMessage(), e);
            }
            if (!result) {
                close();
            }
            return result;
        }

        @Override
        public final Spliterator<SchemaAndValue> trySplit() { // NOPMD returning null is reqruied by API
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.NONNULL;
        }
    }
}
