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
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;

public abstract class Transformer<T> {

    public abstract void configureValueConverter(Map<String, String> config, AbstractConfig sourceConfig);

    public final Stream<T> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig, final long skipRecords) {

        final StreamSpliterator<T> spliterator = createSpliterator(inputStreamIOSupplier, topic, topicPartition,
                sourceConfig);
        return StreamSupport.stream(spliterator, false).onClose(spliterator::close).skip(skipRecords);
    }

    /**
     * Creates the stream spliterator for this transformer.
     *
     * @param inputStreamIOSupplier
     *            the input stream supplier.
     * @param topic
     *            the topic.
     * @param topicPartition
     *            the partition.
     * @param sourceConfig
     *            the source configuraiton.
     * @return a StreamSpliterator instance.
     */
    protected abstract StreamSpliterator<T> createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier,
            String topic, int topicPartition, AbstractConfig sourceConfig);

    public abstract SchemaAndValue getValueData(T record, String topic, AbstractConfig sourceConfig);

    public abstract SchemaAndValue getKeyData(Object cloudStorageKey, String topic, AbstractConfig sourceConfig);

    /**
     * A Spliterator that performs various checks on the opening/closing of the input stream.
     *
     * @param <T>
     *            the type of item created by this Spliterator.
     */
    protected abstract static class StreamSpliterator<T> implements Spliterator<T> {
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
         * A flag indicate that the input stream has been closed.
         */
        private boolean closed;

        /**
         * Constructor.
         *
         * @param logger
         *            The logger for this Spliterator to use.
         * @param inputStreamIOSupplier
         *            the InputStream supplier
         */
        protected StreamSpliterator(final Logger logger, final IOSupplier<InputStream> inputStreamIOSupplier) {
            this.logger = logger;
            this.inputStreamIOSupplier = inputStreamIOSupplier;
        }

        /**
         * Attempt to read the next record. If there is no record to read or an error occurred return false. If a record
         * was created, call {@code action.accept()} with the record.
         *
         * @param action
         *            the Consumer to call if record is created.
         * @return {@code true} if a record was processed, {@code false} otherwise.
         */
        abstract protected boolean doAdvance(Consumer<? super T> action);

        /**
         * Method to close additional inputs if needed.
         */
        abstract protected void doClose();

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
         * Allows modification of input stream. Called immediatly after the input stream is opened. Implementations may
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
        public final boolean tryAdvance(final Consumer<? super T> action) {
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
            } catch (RuntimeException e) { // NOPMD must catch runtime exception here.
                logger.error("Error trying to advance data: {}", e.getMessage(), e);
            }
            if (!result) {
                close();
            }
            return result;
        }

        @Override
        public final Spliterator<T> trySplit() { // NOPMD returning null is reqruied by API
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
