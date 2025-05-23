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

package io.aiven.kafka.connect.common.integration;

import java.io.InputStream;
import java.util.List;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.NativeInfo;

import org.apache.commons.io.function.IOSupplier;

/**
 * The base class for Sink and Source storage.
 *
 * @param <N>
 *            the native storage object type
 * @param <K>
 *            the native storage key type.
 */
public interface StorageBase<K extends Comparable<K>, N> {
    /**
     * Gets the Connector class under test.
     *
     * @return The Connector class under test.
     */
    Class<? extends Connector> getConnectorClass();

    /**
     * Creates the storage space
     */
    void createStorage();

    /**
     * Deletes the storage space.
     */
    void removeStorage();
    /**
     * Retrieves a list of {@link NativeInfo} implementations, one for each item in native storage.
     *
     * @return the list of {@link NativeInfo} implementations, one for each item in native storage.
     */
    List<? extends NativeInfo<N, K>> getNativeStorage();

    /**
     * Gets an IOSupplier for an InputStream for the specified nativeKey.
     *
     * @param nativeKey
     *            the key to retrieve the contents for.
     * @return An IOSupplier for an InputStream for the specified nativeKey.
     */
    IOSupplier<InputStream> getInputStream(K nativeKey);

    /**
     * Gets the default prefix used in testing.
     * <p>
     * Note - This method may be unnecessary.
     * </p>
     *
     * @return The default prefix used in testing.
     */
    String defaultPrefix();
}
