/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;

import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import io.aiven.kafka.connect.common.source.NativeInfo;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.github.tomakehurst.wiremock.WireMockServer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * S3 implementation of SinkStorage.
 */
public class S3SinkStorage implements SinkStorage<S3Object, String> {
    /** The Access key */
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    /** The access secret */
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    /** the test bucket name */
    private static final String TEST_BUCKET_NAME = "test-bucket0";
    /** the S3 container */
    private final LocalStackContainer container;
    /** THe bucket accessor */
    private final BucketAccessor bucketAccessor;

    /**
     * Creates the container.
     * @return the S3 Container.
     */
    public static LocalStackContainer createContainer() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    /**
     * Constructor.
     * @param container the container to execute against.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public S3SinkStorage(final LocalStackContainer container) {
        this.container = container;
        bucketAccessor = new BucketAccessor(createS3Client(container), TEST_BUCKET_NAME);
    }

    /**
     * Creates an AmazonS3 client.
     * @param localStackContainer the container containing the S3 server.
     * @return an AmazonS3 client.
     */
    private AmazonS3 createS3Client(final LocalStackContainer localStackContainer) {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    @Override
    public String defaultPrefix() {
        return "";
    }

    @Override
    public String getAvroBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d.avro", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        // TODO FIX THIS !!! We should not be formatting the output string based on the presence of the prefix.
        final String result = StringUtils.isBlank(prefix)
                ? String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset)
                : String.format("%s%s-%d-%020d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getKeyBlobName(final String prefix, final String key, final CompressionType compression) {
        final String result = String.format("%s%s", prefix, key);
        return result + compression.extension();
    }

    @Override
    public String getNewBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public String getTimestampBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s-%s-%s", prefix, topicName, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")), time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd")));
    }

    @Override
    public Map<String, String> createSinkProperties(final String prefix, final String connectorName) {
        final Map<String, String> config = new HashMap<>();
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
    public String getURLPathPattern(final String topicName) {
        return String.format("/%s/%s([\\-0-9]+)", TEST_BUCKET_NAME, topicName);
    }

    @Override
    public boolean enableProxy(final Map<String, String> config, final WireMockServer proxy) {
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
    public IOSupplier<InputStream> getInputStream(final String nativeKey) {
        return bucketAccessor.getStream(nativeKey);
    }
}
