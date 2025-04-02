package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.integration.AbstractSourceIteratorIntegrationTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testdata.AWSIntegrationTestData;
import io.aiven.kafka.connect.s3.source.testdata.S3OffsetManagerIntegrationTestData;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecordIterator;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@Testcontainers
public class S3SourceRecordIteratorIntegrationTest extends AbstractSourceIteratorIntegrationTest<String, S3OffsetManagerEntry, S3SourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceRecordIteratorIntegrationTest.class);

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
            .withServices(LocalStackContainer.Service.S3);

    AWSIntegrationTestData testData;

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAWS() {
        testData = new AWSIntegrationTestData(LOCALSTACK);
    }

    @AfterEach
    void tearDownAWS() {
        testData.tearDown();
    }


    /**
     * Creates the native key.
     * @param prefix the prefix for the key.
     * @param topic the topic for the key,
     * @param partition the partition for the key.
     * @return the native Key.
     */
    @Override
    protected String createKey(String prefix, String topic, int partition) {
        return testData.createKey(prefix, topic, partition);
    }

    @Override
    protected List<BucketAccessor.S3NativeInfo> getNativeStorage() {
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
    protected BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }

    @Override
    protected OffsetManager.OffsetManagerKey createOffsetManagerKey(final String nativeKey) {
        return S3OffsetManagerIntegrationTestData.createOffsetManagerKey(nativeKey);
    }

    @Override
    protected Function<Map<String, Object>, S3OffsetManagerEntry> getOffsetManagerEntryCreator(final OffsetManager.OffsetManagerKey key) {
        return  S3OffsetManagerIntegrationTestData.getOffsetManagerEntryCreator(key);
    }

    @Override
    protected S3SourceRecordIterator getSourceRecordIterator(final Map<String, String> configData, final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer) {
        S3SourceConfig sourceConfig = new S3SourceConfig(configData);
        return new S3SourceRecordIterator(sourceConfig, offsetManager, transformer, new AWSV2SourceClient(sourceConfig));
    }
}
