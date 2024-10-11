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
import java.util.function.Function;


/**
 * Decorates an iterator such that each element returned is transformed.
 *
 * @param <I> the type of the input to the function.
 * @param <O> the type of the result of the function.
 * @since 1.0
 */
public class TransformIterator<I, O> implements Iterator<O> {

    /** The iterator being used */
    private Iterator<? extends I> iterator;
    /** The transformer being used */
    private Function<? super I, ? extends O> transformer;


    /**
     * Constructs a new {@code TransformIterator} that will use the
     * given iterator and transformer.  If the given transformer is null,
     * then objects will not be transformed.
     *
     * @param iterator  the iterator to use
     * @param transformer  the transformer to use
     */
    public TransformIterator(final Iterator<? extends I> iterator,
                             final Function<? super I, ? extends O> transformer) {
        this.iterator = iterator;
        this.transformer = transformer;
    }

    protected TransformIterator(final Iterator<? extends I> iterator) {
        this.iterator = iterator;
    }

    protected void setTransform(final Function<? super I, ? extends O> transformer) {
        this.transformer = transformer;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    /**
     * Gets the next object from the iteration, transforming it using the
     * current transformer. If the transformer is null, no transformation
     * occurs and the object from the iterator is returned directly.
     *
     * @return the next object
     * @throws java.util.NoSuchElementException if there are no more elements
     */
    @Override
    public O next() {
        return transformer.apply(iterator.next());
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
