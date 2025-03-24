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

package io.aiven.kafka.connect.common.source.input;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.aiven.kafka.connect.common.source.AbstractSourceRecordTest;
import io.aiven.kafka.connect.common.source.impl.ExampleNativeObject;
import io.aiven.kafka.connect.common.source.impl.ExampleOffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.ExampleSourceRecord;

/**
 * A test class to verify the AbstractSourceRecordTest works.
 */
@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class ExampleSourceRecordTest
        extends
            AbstractSourceRecordTest<ExampleNativeObject, String, ExampleOffsetManagerEntry, ExampleSourceRecord> {

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected ExampleOffsetManagerEntry createOffsetManagerEntry(final String key) {
        return new ExampleOffsetManagerEntry(key, "three");
    }

    @Override
    protected ExampleSourceRecord createSourceRecord() {
        return new ExampleSourceRecord(
                new ExampleNativeObject("key", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8))));
    }

}
