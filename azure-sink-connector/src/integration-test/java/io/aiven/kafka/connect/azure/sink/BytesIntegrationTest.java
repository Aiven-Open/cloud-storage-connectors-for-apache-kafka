package io.aiven.kafka.connect.azure.sink;

import com.azure.storage.blob.models.BlobItem;
import io.aiven.kafka.connect.common.integration.sink.AbstractBytesIntegrationTest;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class BytesIntegrationTest extends AbstractBytesIntegrationTest<BlobItem, String> {

    /**
     * The azure container
     */
    @Container
    private static final AzuriteContainer AZURITE_CONTAINER = AzureSinkStorage.createContainer();

    private AzureSinkStorage storage;

    public BytesIntegrationTest() {
        storage = new AzureSinkStorage(AZURITE_CONTAINER);
    }
    @Override
    protected AzureSinkStorage getSinkStorage() {
        return storage;
    }
}
