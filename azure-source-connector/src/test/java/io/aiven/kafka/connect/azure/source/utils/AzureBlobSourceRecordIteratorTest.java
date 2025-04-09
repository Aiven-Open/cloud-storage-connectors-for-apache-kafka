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

package io.aiven.kafka.connect.azure.source.utils;

import com.azure.core.util.io.IOUtils;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import reactor.core.publisher.Flux;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
final public class AzureBlobSourceRecordIteratorTest
        extends
            AbstractSourceRecordIteratorTest<BlobItem, String, AzureBlobOffsetManagerEntry, AzureBlobSourceRecord> {

    private AzureBlobClient azureBlobClient;

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected AbstractSourceRecordIterator<BlobItem, String, AzureBlobOffsetManagerEntry, AzureBlobSourceRecord> createSourceRecordIterator(
            final SourceCommonConfig mockConfig, final OffsetManager<AzureBlobOffsetManagerEntry> offsetManager,
            final Transformer transformer) {
        return new AzureBlobSourceRecordIterator((AzureBlobSourceConfig) mockConfig, offsetManager, transformer,
                azureBlobClient);
    }

    @Override
    protected ClientMutator<BlobItem, String, ?> createClientMutator() {
        return new Mutator();
    }

    @Override
    protected SourceCommonConfig createMockedConfig() {
        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
        when(config.getAzureContainerName()).thenReturn("container1");
        return config;
    }

    private class Mutator extends AbstractSourceRecordIteratorTest.ClientMutator<BlobItem, String, Mutator> {

        @Override
        protected BlobItem createObject(final String key, final ByteBuffer data) {
            final BlobItem blobItem = new BlobItem();
            blobItem.setName(key);
            final BlobItemProperties blobItemProperties = new BlobItemProperties();
            blobItemProperties.setContentLength((long) data.capacity());
            blobItem.setProperties(blobItemProperties);
            return blobItem;
        }

        /**
         * Create stream of BlobItems from a single block.
         *
         * @return the new Stream of BlobItems
         */
        private Stream<BlobItem> dequeueData() {
            // Dequeue a block. Sets the objects.
            dequeueBlock();
            return objects.stream();
        }

        private InputStream getStream(final String key) {
            final ByteBuffer buffer = getData(key);
            return (buffer != null) ? new ByteBufferInputStream(buffer) : new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public void build() {
            // if there are objects create the last block from them.
            if (!objects.isEmpty()) {
                endOfBlock();
            }

            azureBlobClient = mock(AzureBlobClient.class);
            when(azureBlobClient.getAzureBlobStream()).thenAnswer(env -> dequeueData());
            when(azureBlobClient.getBlob(anyString())).thenAnswer(env -> getStream(env.getArgument(0)));
        }
    }
}
