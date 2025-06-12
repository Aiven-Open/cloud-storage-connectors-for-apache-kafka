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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A "native" object for testing.
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public class ExampleNativeObject {
    // instance vars are package private.
    final String key;
    final ByteBuffer data;

    /**
     * Constructor.
     *
     * @param key
     *            the key for this object.
     * @param data
     *            the data for this object.
     */
    public ExampleNativeObject(final String key, final ByteBuffer data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getData() {
        return data;
    }
}
