package io.aiven.kafka.connect.s3.source.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class FilterIterator<E> implements Iterator<E> {

    /** The iterator being used */
    private Iterator<? extends E> iterator;
    /** The predicate being used */
    private Predicate<? super E> predicate;
    /** The next object in the iteration */
    private E nextObject;
    /** Whether the next object has been calculated yet */
    private boolean nextObjectSet;

    /**
     * Constructs a new {@code FilterIterator} that will use the
     * given iterator and predicate.
     *
     * @param iterator  the iterator to use
     * @param predicate  the predicate to use
     */
    public FilterIterator(final Iterator<? extends E> iterator, final Predicate<? super E> predicate) {
        this.iterator = iterator;
        this.predicate = predicate;
    }

    /**
     * Returns true if the underlying iterator contains an object that
     * matches the predicate.
     *
     * @return true if there is another object that matches the predicate
     * @throws NullPointerException if either the iterator or predicate are null
     */
    @Override
    public boolean hasNext() {
        return nextObjectSet || setNextObject();
    }

    /**
     * Returns the next object that matches the predicate.
     *
     * @return the next object which matches the given predicate
     * @throws NullPointerException if either the iterator or predicate are null
     * @throws NoSuchElementException if there are no more elements that
     *  match the predicate
     */
    @Override
    public E next() {
        if (!nextObjectSet && !setNextObject()) {
            throw new NoSuchElementException();
        }
        nextObjectSet = false;
        return nextObject;
    }

    /**
     * Removes from the underlying collection of the base iterator the last
     * element returned by this iterator.
     * This method can only be called
     * if {@code next()} was called, but not after
     * {@code hasNext()}, because the {@code hasNext()} call
     * changes the base iterator.
     *
     * @throws IllegalStateException if {@code hasNext()} has already
     *  been called.
     */
    @Override
    public void remove() {
        if (nextObjectSet) {
            throw new IllegalStateException("remove() cannot be called");
        }
        iterator.remove();
    }

    /**
     * Sets the iterator for this iterator to use.
     * If iteration has started, this effectively resets the iterator.
     *
     * @param iterator  the iterator to use
     */
    public void setIterator(final Iterator<? extends E> iterator) {
        this.iterator = iterator;
        nextObject = null;
        nextObjectSet = false;
    }

    /**
     * Sets nextObject to the next object. If there are no more
     * objects, then return false. Otherwise, return true.
     */
    private boolean setNextObject() {
        while (iterator.hasNext()) {
            final E object = iterator.next();
            if (predicate.test(object)) {
                nextObject = object;
                nextObjectSet = true;
                return true;
            }
        }
        return false;
    }
}
