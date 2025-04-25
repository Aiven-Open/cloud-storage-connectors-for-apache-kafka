package io.aiven.kafka.connect;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import io.aiven.kafka.connect.common.source.NativeInfo;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;

public class S3SinkStorage implements SinkStorage<S3Object, String> {

    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private final LocalStackContainer container;
    private final AmazonS3 s3Client;
    private final BucketAccessor bucketAccessor;


    public static LocalStackContainer createContainer() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    public S3SinkStorage(LocalStackContainer container) {
        this.container = container;
        s3Client = S3IntegrationHelper.createS3Client(container);
        bucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
    }

    @Override
    public String defaultPrefix() {
        return "";
    }

    @Override
    public String getAvroBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        final String result = String.format("%s%s-%d-%d.avro", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getKeyBlobName(String prefix, String key, CompressionType compression) {
        final String result = String.format("%s%s", prefix, key);
        return result + compression.extension();
    }

    @Override
    public String getNewBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getTimestampBlobName(String prefix, String topicName, int partition, int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s-%s-%s", prefix, topicName, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")), time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd")));
    }

    @Override
    public Map<String, String> createSinkProperties(String prefix, String connectorName) {
         Map<String, String> config = new HashMap<>();
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", getEndpointURL());
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        if (StringUtils.isNotBlank(prefix)) {
            config.put("aws.s3.prefix", prefix);
        }
        return config;
    }

    @Override
    public String getEndpointURL() {
        return container.getEndpoint().toString();
    }

    @Override
    public String getURLPathPattern(String topicName) {
        return String.format("/%s/%s([\\-0-9]+)", TEST_BUCKET_NAME, topicName);
    }

    @Override
    public boolean enableProxy(Map<String, String> config, WireMockServer proxy) {
        System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        config.put("aws.s3.endpoint", proxy.baseUrl());
        return true;
    }

    @Override
    public CompressionType getDefaultCompression() {
        return CompressionType.GZIP;
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return AivenKafkaConnectS3SinkConnector.class;
    }

    @Override
    public void createStorage() {
        bucketAccessor.createBucket();
    }

    @Override
    public void removeStorage() {
        bucketAccessor.removeBucket();
    }

    @Override
    public List<? extends NativeInfo<S3Object, String>> getNativeStorage() {
        return bucketAccessor.getNativeInfo();
    }

    @Override
    public IOSupplier<InputStream> getInputStream(String nativeKey) {
        return bucketAccessor.getStream(nativeKey);
    }
}
