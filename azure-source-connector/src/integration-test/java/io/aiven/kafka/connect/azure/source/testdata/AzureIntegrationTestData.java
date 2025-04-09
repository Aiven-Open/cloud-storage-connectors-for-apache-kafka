package io.aiven.kafka.connect.azure.source.testdata;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.azure.source.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.azure.source.testutils.ContainerAccessor;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
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
    static final String AZURE_CONTAINER = "test-container";
    private static final int AZURE_BLOB_PORT = 10_000;
    private static final int AZURE_QUEUE_PORT = 10_001;
    private static final int AZURE_TABLE_PORT = 10_002;
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final GenericContainer<?> container;
    private final Map<Integer, Integer> portMap;
    private final BlobServiceClient azureServiceClient;
    private final AzureBlobAccessor azureBlobAccessor;
    private final ContainerAccessor containerAccessor;;

    public AzureIntegrationTestData(GenericContainer<?> container) {
        this.container = container;
        portMap = new HashMap<>();
        for (String binding : container.getPortBindings()) {
            String[] parts = binding.split(":");
            String[] parts2 = parts[1].split("/");
            int externalPort = Integer.parseInt(parts[0]);
            int internalPort = Integer.parseInt(parts2[0]);
            portMap.put(internalPort, externalPort);
        }

        String azureEndpoint = String.format(
                    "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", ACCOUNT_NAME,
                    ACCOUNT_KEY, getBlobEndpoint(), ACCOUNT_NAME);

        azureServiceClient = new BlobServiceClientBuilder().connectionString(azureEndpoint).buildClient();
        containerAccessor = new ContainerAccessor(azureServiceClient, AZURE_CONTAINER);
        containerAccessor.createContainer();
        azureBlobAccessor = new AzureBlobAccessor(containerAccessor);
    }

    public int getBlobPort() {
        return portMap.get(AZURE_BLOB_PORT);
    }

    public int getQueuePort() {
        return portMap.get(AZURE_QUEUE_PORT);
    }

    public int getTablePort() {
        return portMap.get(AZURE_TABLE_PORT);
    }

    public String getBlobEndpoint() {
        return format("http://%s:%s", container.getContainerIpAddress(), getBlobPort());
    }

    public String getQueueEndpoint() {
        return format("http://%s:%s", container.getContainerIpAddress(), getQueuePort());
    }

    public String getTableEndpoint() {
        return format("http://%s:%s", container.getContainerIpAddress(), getTablePort());
    }


    public void tearDown() {
        containerAccessor.removeContainer();
    }

    /**
     * Finds 3 simultaneously free port for Kafka listeners
     *
     * @return list of 2 ports
     * @throws IOException
     *             when port allocation failure happens
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

    public static GenericContainer<?> createContainer() {
        try {
            List<Integer> ports = findListenerPorts(3);
            return new FixedHostPortGenericContainer<>(
                    "mcr.microsoft.com/azure-storage/azurite")
                    .withFixedExposedPort(ports.get(0), AZURE_BLOB_PORT)
                    .withFixedExposedPort(ports.get(1), AZURE_QUEUE_PORT)
                    .withFixedExposedPort(ports.get(2), AZURE_TABLE_PORT)
                    .withCommand("azurite --blobHost 0.0.0.0  --queueHost 0.0.0.0 --tableHost 0.0.0.0")
                    .withReuse(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
        Map<String, String> data = new HashMap<>();

        SourceConfigFragment.setter(data)
                .ringBufferSize(10);

        AzureBlobConfigFragment.Setter setter =  AzureBlobConfigFragment.setter(data)
                .containerName(AZURE_CONTAINER)
                .connectionString(String.format(
                        "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", ACCOUNT_NAME,
                        ACCOUNT_KEY, getBlobEndpoint(), ACCOUNT_NAME));
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }
}

