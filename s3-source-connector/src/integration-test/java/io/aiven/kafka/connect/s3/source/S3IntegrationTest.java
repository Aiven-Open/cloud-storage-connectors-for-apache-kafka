package io.aiven.kafka.connect.s3.source;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

import java.io.IOException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

@Testcontainers
public final class S3IntegrationTest extends AbstractSourceIntegrationTest<String, S3OffsetManagerEntry, S3SourceRecordIterator> {
    private static final String BUCKET_NAME = "test-bucket";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-AWS-test-";
    private static final String S3_ACCESS_KEY_ID = "test-key-id";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key";
    private static final String CONNECTOR_NAME = "s3-source-connector";

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
            .withServices(LocalStackContainer.Service.S3);


    private static String s3Prefix;

    private S3Client s3Client;

    private BucketAccessor testBucketAccessor;


    @Override
    final protected String getPrefix() {
        return s3Prefix;
    }


    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
        extractConnectorPlugin(getPluginDir("s3-source-connector-for-apache-kafka"));
    }


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
    final protected String createKey(String prefix, String topic, int partition) {
        //{{topic}}-{{partition}}-{{timestamp}}
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }

    @Override
    final protected List<BucketAccessor.S3NativeInfo> getNativeStorage() {
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
        S3ConfigFragment.setter(configData)
                .accessKeyId(S3_ACCESS_KEY_ID)
                .endpoint(LOCALSTACK.getEndpoint())
                .bucketName(BUCKET_NAME)
                .prefix(s3Prefix)
                .fetchBufferSize(10);

        S3SourceConfig sourceConfig = new S3SourceConfig(configData);
        return new S3SourceRecordIterator(sourceConfig, offsetManager, transformer, new AWSV2SourceClient(sourceConfig));
    }
}
