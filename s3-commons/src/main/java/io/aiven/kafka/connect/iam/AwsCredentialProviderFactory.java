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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

public class AwsCredentialProviderFactory {

    public AWSCredentialsProvider getProvider(final S3ConfigFragment config) {
        if (config.hasAwsStsRole()) {
            return getStsProvider(config);
        }
        final BasicAWSCredentials awsCredentials = config.getAwsCredentials();
        if (Objects.isNull(awsCredentials)) {
            return config.getCustomCredentialsProvider();
        }
        return new AWSStaticCredentialsProvider(awsCredentials);
    }

    private AWSCredentialsProvider getStsProvider(final S3ConfigFragment config) {
        final AwsStsRole awsstsRole = config.getStsRole();
        final AWSSecurityTokenService sts = securityTokenService(config);
        return new STSAssumeRoleSessionCredentialsProvider.Builder(awsstsRole.getArn(), awsstsRole.getSessionName())
                .withStsClient(sts)
                .withExternalId(awsstsRole.getExternalId())
                .withRoleSessionDurationSeconds(awsstsRole.getSessionDurationSeconds())
                .build();
    }

    private AWSSecurityTokenService securityTokenService(final S3ConfigFragment config) {
        if (config.hasStsEndpointConfig()) {
            final AWSSecurityTokenServiceClientBuilder stsBuilder = AWSSecurityTokenServiceClientBuilder.standard();
            stsBuilder.setEndpointConfiguration(config.getAwsEndpointConfiguration());
            return stsBuilder.build();
        }
        return AWSSecurityTokenServiceClientBuilder.defaultClient();
    }
}
