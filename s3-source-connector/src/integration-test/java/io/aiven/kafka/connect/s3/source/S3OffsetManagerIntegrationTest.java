package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.integration.AbstractOffsetManagerIntegrationTest;
import io.aiven.kafka.connect.s3.source.testdata.AWSIntegrationTestData;
import io.aiven.kafka.connect.s3.source.testdata.S3OffsetManagerIntegrationTestData;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
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

@Testcontainers
public class S3OffsetManagerIntegrationTest extends AbstractOffsetManagerIntegrationTest<String, S3OffsetManagerEntry, S3SourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3OffsetManagerIntegrationTest.class);

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
    protected Map<String, String> createConnectorConfig(String localPrefix) {
        return testData.createConnectorConfig(localPrefix);
    }

    @Override
    protected BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }
}
