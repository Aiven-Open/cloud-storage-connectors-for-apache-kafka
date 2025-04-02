package io.aiven.kafka.connect.s3.source.testdata;

import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest.WriteResult;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.S3SourceConnector;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.testcontainers.containers.localstack.LocalStackContainer;
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

import static java.lang.String.format;

public class AWSIntegrationTestData {
    static final String BUCKET_NAME = "test-bucket";
    static final String S3_ACCESS_KEY_ID = "test-key-id";
    static final String S3_SECRET_ACCESS_KEY = "test_secret_key";

    private final LocalStackContainer container;

    private final BucketAccessor testBucketAccessor;
    protected final S3Client s3Client;

    public AWSIntegrationTestData(LocalStackContainer container) {
        this.container = container;
        s3Client = S3Client.builder()
                .endpointOverride(
                        URI.create(container.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(container.getAccessKey(), container.getSecretKey())))
                .build();
        testBucketAccessor = new BucketAccessor(s3Client, BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    public void tearDown() {
        testBucketAccessor.removeBucket();
        s3Client.close();
    }

    public String createKey(final String prefix, final String topic, final int partition) {
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }


    public WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(nativeKey)
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(testDataBytes));
        return new WriteResult<>(new S3OffsetManagerEntry(BUCKET_NAME, nativeKey).getManagerKey(), nativeKey);
    }

    public List<BucketAccessor.S3NativeInfo> getNativeStorage() {
        return testBucketAccessor.getNativeStorage();
    }

    public Class<? extends Connector> getConnectorClass() {
        return S3SourceConnector.class;
    }

    public Map<String, String> createConnectorConfig(String localPrefix) {
        Map<String, String> data = new HashMap<>();

        S3ConfigFragment.Setter setter =  S3ConfigFragment.setter(data)
                .bucketName(BUCKET_NAME)
                .endpoint(container.getEndpoint())
                .accessKeyId(S3_ACCESS_KEY_ID)
                .accessKeySecret(S3_SECRET_ACCESS_KEY)
                .fetchBufferSize(1);
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }


}
