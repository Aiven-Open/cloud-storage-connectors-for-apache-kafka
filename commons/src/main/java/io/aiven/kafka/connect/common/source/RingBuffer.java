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

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.collections4.queue.SynchronizedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a ring buffer of items.
 * @param <K> the type of item in the queue.  Must support equality check.
 */
public final class RingBuffer<K> extends SynchronizedQueue<K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RingBuffer.class);

    /**
     * Flag to indicate ring buffer should always be empty.
     */
    private final boolean isEmpty;

    /**
     * Create a Ring Buffer of a maximum Size.
     * If the size is less than or equal to 0 then the buffer is always empty.
     *
     * @param size
     *            The size that the linked list should be.
     */
    public RingBuffer(final int size) {
        // TODO explore if this should be backed by some sort of LRU cache.
        super(new CircularFifoQueue<>(size > 0 ? size : 1));
        isEmpty = size <= 0;
    }

    /**
     * Adds a new item if it is not already present.
     * <ul>
     *     <li>If the buffer is always empty the item is ignored and not enqueued.</li>
     *     <li>If the buffer already contains the item it is ignored and not enqueued.</li>
     *     <li>If the buffer is full the oldest entry in the list is removed.</li>
     * </ul>
     *
     * @param item
     *            Item T which is to be added to the Queue
     */
    public void enqueue(final K item) {
        if (!isEmpty && item != null && !contains(item)) {
            if (isFull()) {
                LOGGER.debug("Ring buffer is full");
                poll();
            }
            add(item);
            LOGGER.debug("Ring buffer added item {} record count {}", item, size());
        }
    }

    /**
     * Returns {@code true} if the buffer is full.
     * @return {@code true} if the buffer is full.
     */
    public boolean isFull() {
        return ((CircularFifoQueue<K>) decorated()).isAtFullCapacity();
    }
    /**
     * Get the last value in the Ring buffer
     *
     * @return A value T from the last place in the list, returns null if list is not full.
     */
    public K getOldest() {
        final K oldest = isFull() ? peek() : null;
        LOGGER.debug("Ring buffer getOldest {}", oldest);
        return oldest;
    }
}
