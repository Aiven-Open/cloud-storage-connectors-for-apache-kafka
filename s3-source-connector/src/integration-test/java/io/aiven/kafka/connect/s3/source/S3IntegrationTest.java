package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.integration.AbstractSourceIntegrationTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecordIterator;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

@Testcontainers
public final class S3IntegrationTest extends AbstractSourceIntegrationTest<String, S3OffsetManagerEntry, S3SourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3IntegrationTest.class);
    private static final String BUCKET_NAME = "test-bucket";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-AWS-test-";
    private static final String S3_ACCESS_KEY_ID = "test-key-id";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key";
    private static final String CONNECTOR_NAME = "s3-source-connector";

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
            .withServices(LocalStackContainer.Service.S3);

    private S3Client s3Client;

    private BucketAccessor testBucketAccessor;

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }


//    @BeforeAll
//    static void setUpAll() throws IOException, InterruptedException {
//        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
//    }


    @BeforeEach
    void setupAWS() {
        s3Client = S3Client.builder()
                .endpointOverride(
                        URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
        testBucketAccessor = new BucketAccessor(s3Client, BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    @AfterEach
    void tearDownAWS() {
        testBucketAccessor.removeBucket();
        s3Client.close();
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
        //{{topic}}-{{partition}}-{{timestamp}}
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }

    @Override
    protected List<BucketAccessor.S3NativeInfo> getNativeStorage() {
        return testBucketAccessor.getNativeStorage();
    }

    @Override
    protected BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return this::getOffsetManagerEntry;
    }

    private S3OffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key, final Map<String, Object> data) {
        S3OffsetManagerEntry entry = new S3OffsetManagerEntry(key.get(S3OffsetManagerEntry.BUCKET).toString(), key.get(S3OffsetManagerEntry.OBJECT_KEY).toString());
        return entry.fromProperties(data);
    }

    @Override
    protected WriteResult writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(nativeKey)
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(testDataBytes));
        return new WriteResult(new S3OffsetManagerEntry(BUCKET_NAME, nativeKey).getManagerKey(), nativeKey);
    }

    @Override
    protected String getConnectorName() {
        return CONNECTOR_NAME;
    }

    @Override
    protected Map<String, String> createConnectorConfig(String localPrefix) {
        Map<String, String> data = new HashMap<>();

        KafkaFragment.setter(data)
                .connector(S3SourceConnector.class);

        return S3ConfigFragment.setter(data)
                .bucketName(BUCKET_NAME)
                .endpoint(LOCALSTACK.getEndpoint())
                .accessKeyId(S3_ACCESS_KEY_ID)
                .accessKeySecret(S3_SECRET_ACCESS_KEY)
                .prefix(StringUtils.defaultIfBlank(localPrefix, null))
                .fetchBufferSize(10)
                .data();

                      /*
                config.put("connector.class", S3SourceConnector.class.getName());
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        if (addPrefix) {
            config.put(AWS_S3_PREFIX_CONFIG, s3Prefix);
        }
                 */
    }


    private S3OffsetManagerEntry createOffsetManagerEntry(final String nativeKey) {
        return new S3OffsetManagerEntry(BUCKET_NAME, nativeKey);
    }

    @Override
    protected OffsetManager.OffsetManagerKey createOffsetManagerKey(String nativeKey) {
        return createOffsetManagerEntry(nativeKey).getManagerKey();
    }

    @Override
    protected Function<Map<String, Object>, S3OffsetManagerEntry> getOffsetManagerEntryCreator(OffsetManager.OffsetManagerKey key) {
        return (map) ->  new S3OffsetManagerEntry(key.getPartitionMap().get(S3OffsetManagerEntry.BUCKET).toString(), key.getPartitionMap().get(S3OffsetManagerEntry.OBJECT_KEY).toString()).fromProperties(map);
    }

    @Override
    protected S3SourceRecordIterator getSourceRecordIterator(Map<String, String> configData, OffsetManager<S3OffsetManagerEntry> offsetManager, Transformer transformer) {
        S3SourceConfig sourceConfig = new S3SourceConfig(configData);
        return new S3SourceRecordIterator(sourceConfig, offsetManager, transformer, new AWSV2SourceClient(sourceConfig));
    }
}
