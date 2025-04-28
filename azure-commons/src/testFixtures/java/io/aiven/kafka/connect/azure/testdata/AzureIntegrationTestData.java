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

package io.aiven.kafka.connect.azure.testdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.azure.ContainerAccessor;
import io.aiven.kafka.connect.azure.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;

/**
 * Manages test data
 */
public class AzureIntegrationTestData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureIntegrationTestData.class);
    public static final String DEFAULT_CONTAINER = "test-container";
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    protected final AzuriteContainer container;
    protected final BlobServiceClient azureServiceClient;
    protected final ContainerAccessor containerAccessor;

    /**
     * Creates the Azurite container for testing.
     *
     * @return a newly constructed Azurite container.
     */
    public static AzuriteContainer createContainer() {
        return new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.33.0");
    }

    /**
     * Constructor.
     *
     * @param container
     *            the container to Azure read/wrtie to.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields and avroData")
    public AzureIntegrationTestData(final AzuriteContainer container) {
        this.container = container;
        azureServiceClient = new BlobServiceClientBuilder().connectionString(container.getConnectionString())
                .buildClient();
        LOGGER.info("Azure blob service {} client created", azureServiceClient.getServiceVersion());
        containerAccessor = getContainerAccessor(DEFAULT_CONTAINER);
        containerAccessor.createContainer();
    }

    /**
     * Get a container accessor.
     *
     * @param name
     *            the name of the container.
     * @return a Container accessor.
     */
    public final ContainerAccessor getContainerAccessor(final String name) {
        return new ContainerAccessor(azureServiceClient, name);
    }

    /**
     * release resources from instance.
     */
    public final void releaseResources() {
        containerAccessor.removeContainer();
    }

    /**
     * Creates a native key.
     *
     * @param prefix
     *            the prefix for the key.
     * @param topic
     *            the topic for the key.
     * @param partition
     *            the partition for the key.
     * @return the native key.
     */
    public final String createKey(final String prefix, final String topic, final int partition) {
        return String.format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition,
                System.currentTimeMillis());
    }

    /**
     * Gets the native storage information.
     *
     * @return the native storage information.
     */
    public final List<ContainerAccessor.AzureNativeInfo> getNativeStorage() {
        return containerAccessor.getNativeStorage();
    }

    /**
     * Creates the connector config with the specified local prefix.
     *
     * @param localPrefix
     *            the prefix to prepend to all keys. May be {@code nul}.
     * @return the map of data options.
     */
    public final Map<String, String> createConnectorConfig(final String localPrefix) {
        return createConnectorConfig(localPrefix, DEFAULT_CONTAINER);
    }

    /**
     * Creates the connector config with the specified local prefix and container.
     *
     * @param localPrefix
     *            the prefix to prepend to all keys. May be {@code nul}.
     * @param containerName
     *            the container name to use.
     * @return the map of data options.
     */
    public final Map<String, String> createConnectorConfig(final String localPrefix, final String containerName) {
        final Map<String, String> data = new HashMap<>();
        SourceConfigFragment.setter(data).ringBufferSize(10);

        final AzureBlobConfigFragment.Setter setter = AzureBlobConfigFragment.setter(data)
                .containerName(containerName)
                .accountName(ACCOUNT_NAME)
                .accountKey(ACCOUNT_KEY)
                .endpointProtocol(AzureBlobConfigFragment.Protocol.HTTP)
                .connectionString(container.getConnectionString())
                .containerName(containerName);
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }
}
