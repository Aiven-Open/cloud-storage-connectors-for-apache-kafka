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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest.WriteResult;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.S3SourceConnector;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.testcontainers.containers.localstack.LocalStackContainer;
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

@SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public final class AWSIntegrationTestData {
    static final String BUCKET_NAME = "test-bucket";
    static final String S3_ACCESS_KEY_ID = "test-key-id";
    static final String S3_SECRET_ACCESS_KEY = "test_secret_key";

    private final LocalStackContainer container;

    private final BucketAccessor testBucketAccessor;
    private final S3Client s3Client;

    public static  LocalStackContainer createS3Container() {
       return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2")).withServices(LocalStackContainer.Service.S3);
    }

    public AWSIntegrationTestData(final LocalStackContainer container) {
        this.container = container;
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(container.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())))
                .build();
        testBucketAccessor = new BucketAccessor(s3Client, BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    @SuppressWarnings("PMD.JUnit4TestShouldUseAfterAnnotation")
    public void tearDown() {
        testBucketAccessor.removeBucket();
        s3Client.close();
    }

    public String createKey(final String prefix, final String topic, final int partition) {
        return String.format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition,
                System.currentTimeMillis());
    }

    public WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        final PutObjectRequest request = PutObjectRequest.builder().bucket(BUCKET_NAME).key(nativeKey).build();
        s3Client.putObject(request, RequestBody.fromBytes(testDataBytes));
        return new WriteResult<>(new S3OffsetManagerEntry(BUCKET_NAME, nativeKey).getManagerKey(), nativeKey);
    }

    public List<BucketAccessor.S3NativeInfo> getNativeStorage() {
        return testBucketAccessor.getNativeStorage();
    }

    public Class<? extends Connector> getConnectorClass() {
        return S3SourceConnector.class;
    }

    public Map<String, String> createConnectorConfig(final String localPrefix) {
        final Map<String, String> data = new HashMap<>();

        SourceConfigFragment.setter(data).ringBufferSize(10);

        final S3ConfigFragment.Setter setter = S3ConfigFragment.setter(data)
                .bucketName(BUCKET_NAME)
                .endpoint(container.getEndpoint())
                .accessKeyId(S3_ACCESS_KEY_ID)
                .accessKeySecret(S3_SECRET_ACCESS_KEY);
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }

}
