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

import java.io.InputStream;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.NativeInfo;

import org.apache.commons.io.function.IOSupplier;

public interface NativeSourceData<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>> {
    /**
     * Gets the name for the source type.
     *
     */
    String getSourceName();

    /**
     * Get a stream of Native object from the underlying storage layer. The implementation must return the native
     * objects in a repeatable order based on the key. In addition, the underlying storage must be able to start
     * streaming from a specific previously returned key.
     *
     * @param offset
     *            the native key to start from. May be {@code null} ot indicate start at the beginning.
     * @return A stream of native objects. May be empty but not {@code null}.
     */
    Stream<N> getNativeItemStream(K offset);

    /**
     * Gets an IOSupplier for the specific source record.
     *
     * The implementation should accept an AbstractSourceRecord created from a sourceRecord returned from a previous
     * call to {@link #createSourceRecord}.
     *
     * @param sourceRecord
     *            the source record to get the input stream from.
     * @return the IOSupplier that retrieves an InputStream from the source record.
     */
    IOSupplier<InputStream> getInputStream(T sourceRecord);

    /**
     * Retrieves the native key for the underlying storage that is associated with the native object.
     *
     * @param nativeObject
     *            the native object to retrieve the native key for.
     * @return The native key for the native object.
     */
    K getNativeKey(N nativeObject);

    /**
     * Creates an instance of the concrete implementation of AbstractSourceRecord for the native object. The
     * SAbstractSourceRecord need only contain the {@link NativeInfo} instance.
     *
     * @param nativeObject
     *            the native object to get the AbstractSourceRecord for.
     * @return the AbstractSourceRecord for the native object.
     */
    T createSourceRecord(N nativeObject);

    /**
     * Creates an OffsetManagerEntry for a native object.
     *
     * @param nativeObject
     *            the native object to create the OffsetManagerEntry for.
     * @return An OffsetManagerEntry for a native object.
     */
    O createOffsetManagerEntry(N nativeObject);

    /**
     * Creates an offset manager key for the native key.
     *
     * @param nativeKey
     *            THe native key to create an offset manager key for.
     * @return An offset manager key.
     */
    OffsetManager.OffsetManagerKey getOffsetManagerKey(K nativeKey);

}
