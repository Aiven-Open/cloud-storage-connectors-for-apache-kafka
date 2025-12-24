/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.iam;

import java.util.Objects;

import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Creates AwsCredentialProviders.
 */
public class AwsCredentialProviderFactory {

    /**
     * Gets an AWS V2 credential provider
     *
     * @param config
     *            the S3Configuration fragment.
     * @return an AwsCredentialsProvider
     */
    public AwsCredentialsProvider getAwsV2Provider(final S3ConfigFragment config) {

        if (config.hasAwsStsRole()) {
            return getV2StsProvider(config);
        }
        final AwsBasicCredentials awsCredentials = config.getAwsCredentialsV2();
        if (Objects.isNull(awsCredentials)) {
            return config.getCustomCredentialsProviderV2();
        }
        return StaticCredentialsProvider.create(awsCredentials);
    }

    /**
     * Gets a V2 STS Provider.
     *
     * @param config
     *            the S3Configuration fragment.
     * @return an StsAssumeRoleCredentialsProvider
     */
    private StsAssumeRoleCredentialsProvider getV2StsProvider(final S3ConfigFragment config) {
        if (config.hasAwsStsRole()) {
            return StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(() -> AssumeRoleRequest.builder()
                            .roleArn(config.getStsRole().getArn())
                            .externalId(config.getStsRole().getExternalId())
                            // Maker this a unique identifier
                            .roleSessionName("AwsV2SDKConnectorSession")
                            .build())
                    .stsClient(StsClient.builder().region(config.getAwsS3RegionV2()).build())
                    .build();
        }
        return StsAssumeRoleCredentialsProvider.builder().build();
    }

}
