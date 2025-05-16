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
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public ExampleNativeObject(final String key, final ByteBuffer data) {
        this.key = key;
        this.data = data;
    }
}
