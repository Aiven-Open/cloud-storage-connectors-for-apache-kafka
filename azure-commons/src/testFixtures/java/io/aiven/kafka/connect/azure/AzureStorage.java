package io.aiven.kafka.connect.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.aiven.kafka.connect.azure.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.StorageBase;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AzureStorage implements StorageBase<BlobItem, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorage.class);
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
    public AzureStorage(final AzuriteContainer container) {
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
    @Override
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

    @Override
    public final void createStorage() {
        containerAccessor.createContainer();
    }

    @Override
    public final void removeStorage() {
        containerAccessor.removeContainer();
    }

    @Override
    public String defaultPrefix() {
        return "";
    }

    @Override
    public final IOSupplier<InputStream> getInputStream(String nativeKey) {
        return () -> {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                containerAccessor.getBlobClient(nativeKey).downloadStream(baos);
                baos.close();
                return new ByteArrayInputStream(baos.toByteArray());
            }
        };
    }
}
