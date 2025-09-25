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

package io.aiven.commons.collections;

import java.util.Objects;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.collections4.queue.SynchronizedQueue;

/**
 * Implements a ring buffer of items. Items are inserted until maximum size is
 * reached and then the earliest items are removed when newer items are added.
 *
 * @param <K>
 *            the type of item in the queue. Must support equality check.
 */
public final class RingBuffer<K> {
    /** How to handle the duplicates in the buffer. */
    public enum DuplicateHandling {
        /** Allow duplicates in the buffer. */
        ALLOW,
        /** Reject (do not add) duplicates to the buffer. */
        REJECT,
        /** Move the duplicate entry to the tail of the buffer. */
        DELETE
    }

    /** The wrapped queue. */
    private final SynchronizedQueue<K> queue;

    private final CircularFifoQueue<K> wrappedQueue;

    /** Flag to indicate ring buffer should always be empty. */
    private final boolean alwaysEmpty;

    /** Flag to allow duplicates in the buffer. */
    private final DuplicateHandling duplicateHandling;

    /**
     * Create a Ring Buffer of a maximum size that rejects duplicates. If the size
     * is less than or equal to 0 then the buffer is always empty.
     *
     * @param size
     *            The maximum size of the ring buffer
     * @see DuplicateHandling#REJECT
     */
    public RingBuffer(final int size) {
        this(size, DuplicateHandling.REJECT);
    }

    /**
     * Create a Ring Buffer of specified maximum size and potentially allowing
     * duplicates. If the size is less than or equal to 0 then the buffer is always
     * empty.
     *
     * @param size
     *            The maximum size of the ring buffer
     * @param duplicateHandling
     *            defines how to handle duplicate values in the buffer.
     */
    public RingBuffer(final int size, final DuplicateHandling duplicateHandling) {
        wrappedQueue = new CircularFifoQueue<>(size > 0 ? size : 1);
        queue = SynchronizedQueue.synchronizedQueue(wrappedQueue);
        alwaysEmpty = size <= 0;
        this.duplicateHandling = duplicateHandling;
    }

    @Override
    public String toString() {
        return String.format("RingBuffer[%s, load %s/%s]", duplicateHandling, queue.size(), wrappedQueue.maxSize());
    }

    /**
     * Adds a new item if it is not already present.
     *
     * <ul>
     * <li>If the buffer is always empty the item is ignored and not enqueued.
     * <li>If the buffer already contains the item it is ignored and not enqueued.
     * <li>If the buffer is full the oldest entry in the buffer is ejected.
     * </ul>
     *
     * @param item
     *            Item T which is to be added to the Queue
     * @return The item that was ejected. May be {@code null}.
     */
    public K add(final K item) {
        Objects.requireNonNull(item, "item");
        if (!alwaysEmpty && checkDuplicates(item)) {
            final K result = isFull() ? queue.poll() : null;
            queue.add(item);
            return result;
        }
        return null;
    }

    /**
     * Removes a single instance of the item from the buffer.
     *
     * @param item
     *            the item to remove.
     */
    public void remove(final K item) {
        queue.remove(item);
    }

    /**
     * Determines if the item is in the buffer.
     *
     * @param item
     *            the item to look for.
     * @return {@code true} if the item is in the buffer, {@code false} othersie.
     */
    public boolean contains(final K item) {
        return queue.contains(item);
    }

    /**
     * Returns but does not remove the head of the buffer.
     *
     * @return the item at the head of the buffer. May be {@code null}.
     */
    public K head() {
        return queue.peek();
    }

    /**
     * Returns but does not remove the teal of the buffer.
     *
     * @return the item at the tail of the buffer. May be {@code null}.
     */
    public K tail() {
        final int size = wrappedQueue.size();
        return size == 0 ? null : wrappedQueue.get(size - 1);
    }

    private boolean checkDuplicates(final K item) {
        switch (duplicateHandling) {
            case ALLOW :
                return true;
            case REJECT :
                return !queue.contains(item);
            case DELETE :
                queue.remove(item);
                return true;
            default :
                throw new IllegalStateException("Unsupported duplicate handling: " + duplicateHandling);
        }
    }

    /**
     * Returns {@code true} if the buffer is full.
     *
     * @return {@code true} if the buffer is full.
     */
    public boolean isFull() {
        return wrappedQueue.isAtFullCapacity();
    }

    /**
     * Gets the next item to be ejected. If the buffer is full this will return the
     * oldest value in the buffer. If the buffer is not full this method will return
     * {@code null}.
     *
     * @return A value T from the last place in the buffer, returns null if buffer
     *         is not full.
     */
    public K getNextEjected() {
        return isFull() ? queue.peek() : null;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        return super.equals(object);
    }

    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
