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

package io.aiven.kafka.connect.azure.source.testdata;

import java.io.ByteArrayInputStream;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.testdata.AzureIntegrationTestData;
import io.aiven.kafka.connect.common.integration.source.AbstractSourceIntegrationBase;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.testcontainers.azure.AzuriteContainer;

/**
 * Manages test data
 */
public final class AzureSourceIntegrationTestData extends AzureIntegrationTestData {
    /**
     * Constructor.
     *
     * @param container
     *            the container to Azure read/wrtie to.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields and avroData")
    public AzureSourceIntegrationTestData(final AzuriteContainer container) {
        super(container);
    }

    /**
     * Writes data to the default container.
     *
     * @param nativeKey
     *            the native key to write
     * @param testDataBytes
     *            the data to write.
     * @return the WriteResults.
     */
    public AbstractSourceIntegrationBase.WriteResult<String> writeWithKey(final String nativeKey,
            final byte[] testDataBytes) {
        containerAccessor.getBlobClient(nativeKey).upload(new ByteArrayInputStream(testDataBytes));
        return new AbstractSourceIntegrationBase.WriteResult<>(
                new AzureBlobOffsetManagerEntry(containerAccessor.getContainerName(), nativeKey).getManagerKey(),
                nativeKey);
    }

    /**
     * Gets the connector class.
     *
     * @return the connector class.
     */
    public Class<? extends Connector> getConnectorClass() {
        return AzureBlobSourceConnector.class;
    }

}
