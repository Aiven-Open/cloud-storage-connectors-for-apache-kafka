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

package aiven.kafka.connect.azure.source;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.azure.AzureStorage;
import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.integration.source.SourceStorage;

import com.azure.storage.blob.models.BlobItem;
import org.testcontainers.azure.AzuriteContainer;

public class AzureSourceStorage extends AzureStorage
        implements
            SourceStorage<String, BlobItem, AzureBlobOffsetManagerEntry> {
    /**
     * Constructor.
     *
     * @param container
     *            the container to Azure read/write to.
     */
    public AzureSourceStorage(final AzuriteContainer container) {
        super(container);
    }

    @Override
    public WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        containerAccessor.getBlobClient(nativeKey).upload(new ByteArrayInputStream(testDataBytes));
        return new WriteResult<>(AzureBlobOffsetManagerEntry.asKey(containerAccessor.getContainerName(), nativeKey),
                nativeKey);
    }

    @Override
    public BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureSourceStorage::getOffsetManagerEntry;
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return AzureBlobSourceConnector.class;
    }

    public void cleanup() {
        releaseResources();
    }

    /**
     * Creates an AzureOffsetManagerEntry from the Kafka defined partitionMap and data.
     *
     * @param key
     *            the Kafka defined partitionMap
     * @param data
     *            the Kafka data map.
     * @return the AzureOffsetManagerEntry.
     */
    public static AzureBlobOffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key,
            final Map<String, Object> data) {
        final AzureBlobOffsetManagerEntry entry = new AzureBlobOffsetManagerEntry(
                key.get(AzureBlobOffsetManagerEntry.CONTAINER).toString(),
                key.get(AzureBlobOffsetManagerEntry.BLOB_NAME).toString());
        return entry.fromProperties(data);
    }
}
