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

import io.aiven.kafka.connect.common.source.NativeInfo;

import org.apache.commons.io.function.IOSupplier;

/**
 * Bsse class for storage implementations.
 * @param <N> The native storage class.
 * @param <K> The natvie key class.
 */
public interface StorageBase<N, K extends Comparable<K>> {
    /**
     * Gets the connector for the class.
     * @return
     */
    Class<? extends Connector> getConnectorClass();
    void createStorage();
    void removeStorage();
    /**
     * Retrieves a list of {@link NativeInfo} implementations, one for each item in native storage.
     *
     * @return the list of {@link NativeInfo} implementations, one for each item in native storage.
     */
    List<? extends NativeInfo<N, K>> getNativeStorage();
    IOSupplier<InputStream> getInputStream(K nativeKey);
    String defaultPrefix();
}
