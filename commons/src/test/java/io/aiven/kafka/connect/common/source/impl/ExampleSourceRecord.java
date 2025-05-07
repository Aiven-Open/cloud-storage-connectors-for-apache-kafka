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

import io.aiven.kafka.connect.common.source.AbstractSourceRecord;

import io.aiven.kafka.connect.common.storage.NativeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AbstractSourceRecord implementation for the NativeObject.
 */
final public class ExampleSourceRecord
        extends
            AbstractSourceRecord<ExampleNativeObject, String, ExampleOffsetManagerEntry, ExampleSourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleSourceRecord.class);

    /**
     * Constructor.
     *
     * @param nativeObject
     *            The native object
     */
    public ExampleSourceRecord(final ExampleNativeObject nativeObject) {
        super(LOGGER, new NativeInfo<ExampleNativeObject, String>() {
            @Override
            public ExampleNativeObject getNativeItem() {
                return nativeObject;
            }

            @Override
            public String getNativeKey() {
                return nativeObject.key;
            }

            @Override
            public long getNativeItemSize() {
                return nativeObject.data.capacity();
            }
        });
    }

    /**
     * A copy constructor.
     *
     * @param source
     *            the source record to copy.
     */
    public ExampleSourceRecord(final ExampleSourceRecord source) {
        super(source);
    }

    @Override
    public ExampleSourceRecord duplicate() {
        return new ExampleSourceRecord(this);
    }
}
