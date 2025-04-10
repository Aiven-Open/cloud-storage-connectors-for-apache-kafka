package io.aiven.kafka.connect.azure.source;

import io.aiven.kafka.connect.azure.source.testdata.AzureIntegrationTestData;
import io.aiven.kafka.connect.azure.source.testdata.AzureOffsetManagerIntegrationTestData;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.testdata.ContainerAccessor;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
import io.aiven.kafka.connect.common.integration.AbstractSourceIteratorIntegrationTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public final class AzureBlobSourceRecordIteratorIntegrationTest extends AbstractSourceIteratorIntegrationTest<String, AzureBlobOffsetManagerEntry, AzureBlobSourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceRecordIteratorIntegrationTest.class);

    @Container
    private static final AzuriteContainer AZURITE_CONTAINER = AzureIntegrationTestData.createContainer();

    private AzureIntegrationTestData testData;

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAzure() {
        testData = new AzureIntegrationTestData(AZURITE_CONTAINER);
    }

    @AfterEach
    void tearDownAzure() {
        testData.tearDown();
    }

    @Override
    protected String createKey(final String prefix, final String topic, final int partition) {
        return testData.createKey(prefix, topic, partition);
    }

    @Override
    protected List<ContainerAccessor.AzureNativeInfo> getNativeStorage() {
        return testData.getNativeStorage();
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return testData.getConnectorClass();
    }


    @Override
    protected WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        return testData.writeWithKey(nativeKey, testDataBytes);
    }

    @Override
    protected Map<String, String> createConnectorConfig(final String localPrefix) {
        return testData.createConnectorConfig(localPrefix);
    }

    @Override
    protected BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureOffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }

    @Override
    protected AzureBlobSourceRecordIterator getSourceRecordIterator(final Map<String, String> configData, final OffsetManager<AzureBlobOffsetManagerEntry> offsetManager, final Transformer transformer) {
        final AzureBlobSourceConfig sourceConfig = new AzureBlobSourceConfig(configData);
        return new AzureBlobSourceRecordIterator(sourceConfig, offsetManager, transformer, new AzureBlobClient(sourceConfig));
    }
}
