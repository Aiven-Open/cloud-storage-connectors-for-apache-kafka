package io.aiven.kafka.connect.azure.source.testdata;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class AzureIntegrationTestData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureIntegrationTestData.class);
    static final String DEFAULT_CONTAINER = "test-container";
    private static final int AZURE_BLOB_PORT = 10_000;
    private static final int AZURE_QUEUE_PORT = 10_001;
    private static final int AZURE_TABLE_PORT = 10_002;
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final AzuriteContainer container;
    private final BlobServiceClient azureServiceClient;
    private final ContainerAccessor containerAccessor;


    public AzureIntegrationTestData(AzuriteContainer container) {
        this.container = container;

        azureServiceClient = new BlobServiceClientBuilder().connectionString(container.getConnectionString()).buildClient();
        LOGGER.info("Azure blob service {} client created", azureServiceClient.getServiceVersion());
        containerAccessor = new ContainerAccessor(azureServiceClient, DEFAULT_CONTAINER);
        containerAccessor.createContainer();
    }

    public ContainerAccessor getContainerAccessor(String name) {
        return new ContainerAccessor(azureServiceClient, name);
    }

    public void tearDown() {
        containerAccessor.removeContainer();
    }

    /**
     * Finds 3 simultaneously free port for Kafka listeners
     *
     * @return list of 2 ports
     * @throws IOException when port allocation failure happens
     */
    static List<Integer> findListenerPorts(int count) throws IOException {
        ServerSocket[] sockets = new ServerSocket[count];
        try {
            for (int i = 0; i < sockets.length; i++) {
                sockets[i] = new ServerSocket(0);
            }
            return Arrays.stream(sockets).map(ServerSocket::getLocalPort).collect(Collectors.toList());
        } catch (IOException e) {
            throw new IOException("Failed to allocate ports for test", e);
        }
    }


    public static AzuriteContainer createContainer() {
        return new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.33.0");
    }

    public String createKey(final String prefix, final String topic, final int partition) {
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }

    public AbstractIntegrationTest.WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        containerAccessor.getBlobClient(nativeKey).upload(new ByteArrayInputStream(testDataBytes));
        return new AbstractIntegrationTest.WriteResult<>(new AzureBlobOffsetManagerEntry(containerAccessor.getContainerName(), nativeKey).getManagerKey(), nativeKey);
    }

    public List<ContainerAccessor.AzureNativeInfo> getNativeStorage() {
        return containerAccessor.getNativeStorage();
    }

    public Class<? extends Connector> getConnectorClass() {
        return AzureBlobSourceConnector.class;
    }

    public Map<String, String> createConnectorConfig(String localPrefix) {
        return createConnectorConfig(localPrefix, DEFAULT_CONTAINER);
    }

    public Map<String, String> createConnectorConfig(String localPrefix, String containerName) {
        Map<String, String> data = new HashMap<>();

        SourceConfigFragment.setter(data)
                .ringBufferSize(10);

        AzureBlobConfigFragment.Setter setter =  AzureBlobConfigFragment.setter(data)
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

