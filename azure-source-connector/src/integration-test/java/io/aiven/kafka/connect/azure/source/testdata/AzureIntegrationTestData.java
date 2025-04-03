package io.aiven.kafka.connect.azure.source.testdata;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.azure.source.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class AzureIntegrationTestData {
    static final String AZURE_CONTAINER = "test-container";
    private static final int AZURE_BLOB_PORT = 10_000;
    private static final int AZURE_QUEUE_PORT = 10_001;
    private static final int AZURE_TABLE_PORT = 10_002;
    private static final String AZURE_ENDPOINT = "http://127.0.0.1:10000";
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final GenericContainer<?> container;

    private final BlobContainerClient containerClient;

    private final AzureBlobAccessor azureBlobAccessor;

    public AzureIntegrationTestData(GenericContainer<?> container) {
        this.container = container;

        String azureEndpoint = String.format(
                    "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", ACCOUNT_NAME,
                    ACCOUNT_KEY, AZURE_ENDPOINT, ACCOUNT_NAME);
        BlobServiceClient azureServiceClient = new BlobServiceClientBuilder().connectionString(azureEndpoint).buildClient();
        containerClient = azureServiceClient.getBlobContainerClient(AZURE_CONTAINER);
        containerClient.createIfNotExists();
        azureBlobAccessor = new AzureBlobAccessor(containerClient);
    }

    public void tearDown() {
    }

    public static GenericContainer<?> createContainer() {
        return new FixedHostPortGenericContainer<>(
                "mcr.microsoft.com/azure-storage/azurite") // NOPMD
                .withFixedExposedPort(AZURE_BLOB_PORT, AZURE_BLOB_PORT)
                .withFixedExposedPort(AZURE_QUEUE_PORT, AZURE_QUEUE_PORT)
                .withFixedExposedPort(AZURE_TABLE_PORT, AZURE_TABLE_PORT)
                .withCommand("azurite --blobHost 0.0.0.0  --queueHost 0.0.0.0 --tableHost 0.0.0.0")
                .withReuse(true);
    }

    public String createKey(final String prefix, final String topic, final int partition) {
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }


    public AbstractIntegrationTest.WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
            containerClient.getBlobClient(nativeKey).upload(new ByteArrayInputStream(testDataBytes));
         return new AbstractIntegrationTest.WriteResult<>(new AzureBlobOffsetManagerEntry(AZURE_CONTAINER, nativeKey).getManagerKey(), nativeKey);
    }

    public List<AzureBlobAccessor.AzureNativeInfo> getNativeStorage() {
        return azureBlobAccessor.getNativeStorage();
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
                        ACCOUNT_KEY, AZURE_ENDPOINT, ACCOUNT_NAME));
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }
}

