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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.Collection;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

public class RingBuffer extends CircularFifoBuffer {
    final private Buffer fifoBuffer;

    /**
     * Create a Ring Buffer of a maximum Size
     *
     * @param size
     *            The size that the linked list should be.
     */
    public RingBuffer(final int size) {
        super(size);
        this.fifoBuffer = BufferUtils.synchronizedBuffer(this);
    }

    /**
     * Create a Ring Buffer from an existing collection
     *
     * @param collection
     *            An existing collection of values
     */
    public RingBuffer(final Collection<Object> collection) {
        super(collection);
        this.fifoBuffer = BufferUtils.synchronizedBuffer(this);
    }

    /**
     * Add a new item if it is not already present in the ring buffer to the ring buffer and removes the last entry from
     * the linked list. Null values are ignored.
     *
     * @param item
     *            Item T which is to be added to the Queue
     */
    public void enqueue(final Object item) {
        if (item != null && !contains(item)) {
            fifoBuffer.add(item);
        }

    }

    /**
     * Get the last value in the Ring buffer
     *
     * @return A value T from the last place in the list, returns null if list is not full.
     */
    public String getOldest() {
        return fifoBuffer.size() == maxSize() ? (String) fifoBuffer.get() : null;
    }
}
