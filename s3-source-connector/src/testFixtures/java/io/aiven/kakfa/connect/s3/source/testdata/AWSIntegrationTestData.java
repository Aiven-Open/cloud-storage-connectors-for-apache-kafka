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

package io.aiven.kakfa.connect.s3.source.testdata;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.source.SourceStorage;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.S3SourceConnector;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Generates data for AWS tests.
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class AWSIntegrationTestData {
    /** The default bucket name */
    static final String BUCKET_NAME = "test-bucket";
    /** The S3 Access Key ID */
    static final String S3_ACCESS_KEY_ID = "test-key-id";
    /** The S3 Access Key secret */
    static final String S3_SECRET_ACCESS_KEY = "test_secret_key";
    /** The local stack container to interact with */
    private final LocalStackContainer container;

    /**
     * The S3Client for testing
     */
    private final S3Client s3Client;

    /**
     * Creates a local stack container. This method is generally used on a static instance variable in the test class
     * that is annotated with the "@Container" annotation.
     *
     * @return the constructed local stack container.
     */
    public static LocalStackContainer createS3Container() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    /**
     * Creates an instance with the container and accessing the default bucket name.
     *
     * @param container
     *            the container to use.
     */
    public AWSIntegrationTestData(final LocalStackContainer container) {
        this.container = container;
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(container.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())))
                .build();
    }

    /**
     * Creates a bucket accessor for the specified bucket.
     *
     * @param bucketName
     *            the bucket name.
     * @return A bucket accessor for the specified bucket.
     */
    public BucketAccessor getBucketAccessor(final String bucketName) {
        final BucketAccessor result = new BucketAccessor(s3Client, bucketName);
        result.createBucket();
        return result;
    }

    /**
     * Creates a bucket accessor for the default bucket.
     *
     * @return A bucket accessor for the default bucket.
     */
    public BucketAccessor getDefaultBucketAccessor() {
        return getBucketAccessor(BUCKET_NAME);
    }

    /**
     * Should be called to clean up the container.
     */
    @SuppressWarnings("PMD.JUnit4TestShouldUseAfterAnnotation")
    public void tearDown() {
        s3Client.close();
    }

    /**
     * Create a key for the container. Key has the format {prefix}{topic}-{partition:5 digit pad}-{timestamp}.txt
     *
     * @param prefix
     *            the prefix
     * @param topic
     *            the topic
     * @param partition
     *            the partition.
     * @return the generated key.
     */
    public String createKey(final String prefix, final String topic, final int partition) {
        return String.format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition,
                System.currentTimeMillis());
    }

    /**
     * Write a record to the datastore.
     *
     * @param nativeKey
     *            the native key.
     * @param testDataBytes
     *            the bytes to write.
     * @return A WriteResult.
     */
    public SourceStorage.WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        final PutObjectRequest request = PutObjectRequest.builder().bucket(BUCKET_NAME).key(nativeKey).build();
        s3Client.putObject(request, RequestBody.fromBytes(testDataBytes));
        return new SourceStorage.WriteResult<>(new S3OffsetManagerEntry(BUCKET_NAME, nativeKey).getManagerKey(),
                nativeKey);
    }

    /**
     * Gets the connector clase.
     *
     * @return the connector class.
     */
    public Class<? extends Connector> getConnectorClass() {
        return S3SourceConnector.class;
    }

    /**
     * Creates a data map of the configuration options for to talk to the container.
     *
     * @param localPrefix
     *            the local prefix if any.
     * @param bucketName
     *            the name of the bucket to write to.
     *
     * @return the data map of the configuration options for to talk to the container.
     */
    public Map<String, String> createConnectorConfig(final String localPrefix, final String bucketName) {
        final Map<String, String> data = new HashMap<>();

        SourceConfigFragment.setter(data).ringBufferSize(10);

        final S3ConfigFragment.Setter setter = S3ConfigFragment.setter(data)
                .bucketName(bucketName)
                .endpoint(container.getEndpoint())
                .accessKeyId(S3_ACCESS_KEY_ID)
                .accessKeySecret(S3_SECRET_ACCESS_KEY);
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }
}
