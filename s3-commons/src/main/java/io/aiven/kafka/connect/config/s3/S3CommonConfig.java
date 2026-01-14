/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.kafka.connect.config.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Common methods needed for S3ClientFactory.
 */
public interface S3CommonConfig {
    /**
     * Gets the AWS region.
     *
     * @return the AWS region.
     */
    Region getAwsS3Region();

    /**
     * Gets the AWS credentials provider.
     *
     * @return the AWS credentials provider.
     */
    AwsCredentialsProvider getAwsV2Provider();

    /**
     * Gets The AWS S3 endpoint. Used to override the default endpoint. May be {@code null}.
     *
     * @return the AWS S3 endpoint to use instead of the normal one.
     */
    String getAwsS3EndPoint();

    /**
     * Gets the delay type to use for retries.
     *
     * @return the delay type.
     */
    S3ConfigFragment.DelayType getDelayType();

    /**
     * Gets the requested delay. Only applies if {@link #getDelayType()} is
     * {@link S3ConfigFragment.DelayType#EXPONENTIAL}.
     *
     * @return the delay in milliseconds.
     */
    long getS3RetryBackoffDelayMs();

    /**
     * Gets the requested maximum delay. Only applies if {@link #getDelayType()} is
     * {@link S3ConfigFragment.DelayType#EXPONENTIAL}.
     *
     * @return the maximum delay in milliseconds.
     */
    long getS3RetryBackoffMaxDelayMs();

    /**
     * Gets the requested maximum number of retries. Only applies if {@link #getDelayType()} is
     * {@link S3ConfigFragment.DelayType#EXPONENTIAL}.
     *
     * @return the maximum number of retries in milliseconds.
     */
    int getS3RetryBackoffMaxRetries();
}
