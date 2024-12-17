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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that returns closable items and will close the last item returned in one of two cases:
 * <ul>
 * <li>when {@link #next} is called.</li>
 * <li>when {@link #hasNext} returns false.</li>
 * </ul>
 *
 * @param <T>
 *            The type of Closeable object to return.
 */
public final class ClosableIterator<T extends Closeable> implements Iterator<T>, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClosableIterator.class);
    private final Iterator<T> delegate;
    private T nextItem;
    private T lastItem;

    public static <T extends Closeable> ClosableIterator<T> wrap(final Iterator<T> iterator) {
        return iterator instanceof ClosableIterator ? (ClosableIterator<T>) iterator : new ClosableIterator<>(iterator);
    }
    private ClosableIterator(final Iterator<T> iterator) {
        delegate = iterator;
    }

    @Override
    public boolean hasNext() {
        if (nextItem == null) {
            if (delegate.hasNext()) {
                nextItem = delegate.next();
            } else {
                closeItem(lastItem);
            }
        }
        return nextItem != null;
    }

    private void closeItem(final T item) {
        if (item != null) {
            try {
                item.close();
            } catch (IOException e) {
                LOGGER.error(String.format("Error closing %s:", item), e);
            }
        }
    }

    @Override
    public T next() {
        closeItem(lastItem);
        lastItem = nextItem;
        nextItem = null; // NOPMD assign null
        return lastItem;
    }

    @Override
    public void close() {
        closeItem(lastItem);
        closeItem(nextItem);
    }
}
