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

package io.aiven.kafka.connect.common.source.impl;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A "native" client. This client returns lists of native objects and ByteBuffers for specific native keys.
 */
public interface ExampleNativeClient {
    /**
     * Gets a list of native objects.
     *
     * @return the list of native objects.
     */
    List<ExampleNativeObject> listObjects(String offset);

    /**
     * Gets the ByteBuffer for a key.
     *
     * @param key
     *            the key to get the byte buffer for.
     * @return The ByteBuffer for a key, or {@code null} if not set.
     */
    ByteBuffer getObjectAsBytes(String key);
}
