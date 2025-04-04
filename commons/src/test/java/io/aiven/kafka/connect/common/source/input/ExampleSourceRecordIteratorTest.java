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

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.impl.ExampleNativeClient;
import io.aiven.kafka.connect.common.source.impl.ExampleNativeObject;
import io.aiven.kafka.connect.common.source.impl.ExampleOffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.ExampleSourceRecord;
import io.aiven.kafka.connect.common.source.impl.ExampleSourceRecordIterator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to verify AbstraactSourceRecordIterator works.
 */
@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class ExampleSourceRecordIteratorTest
        extends
            AbstractSourceRecordIteratorTest<ExampleNativeObject, String, ExampleOffsetManagerEntry, ExampleSourceRecord> {

    ExampleNativeClient nativeClient;

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected ExampleSourceRecordIterator createSourceRecordIterator(final SourceCommonConfig mockConfig,
            final OffsetManager<ExampleOffsetManagerEntry> offsetManager, final Transformer transformer) {
        return new ExampleSourceRecordIterator(mockConfig, offsetManager, transformer, nativeClient);
    }

    @Override
    protected Mutator createClientMutator() {
        return new Mutator();
    }

    @Override
    protected SourceCommonConfig createMockedConfig() {
        return mock(SourceCommonConfig.class);
    }

    //@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public final class Mutator extends ClientMutator<ExampleNativeObject, String, Mutator> {

        @Override
        protected ExampleNativeObject createObject(final String key, final ByteBuffer data) {
            return new ExampleNativeObject(key, data);
        }

        /**
         * Create a list of NativeObjects from a single block.
         *
         * @return A list of NativeObjects from a single block.
         */
        private List<ExampleNativeObject> dequeueData(final String offset) {
            // Dequeue a block. Sets the objects.
            dequeueBlock();
            final boolean matches = offset == null;
            return objects.stream().filter( o -> matches || o.getKey().compareTo(offset) >= 0).collect(Collectors.toList());
        }

        @Override
        public void build() {
            nativeClient = mock(ExampleNativeClient.class);

            // when an object is requested retrieve the answer from the blocks.
            when(nativeClient.listObjects(any())).thenAnswer(env -> dequeueData(env.getArgument(0)));
            // when an objectRequest is sent retrieve the response data.
            when(nativeClient.getObjectAsBytes(anyString())).thenAnswer(env -> getData(env.getArgument(0)));
        }
    }

}
