package io.aiven.kafka.connect.config.s3;

import io.aiven.kafka.connect.iam.AwsStsRole;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Objects;

public interface S3Config {
    // Default values from AWS SDK, since they are hidden
    int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    // Comment in AWS SDK for max retries:
    // Maximum retry limit. Avoids integer overflow issues.
    //
    // NOTE: If the value is greater than 30, there can be integer overflow
    // issues during delay calculation.
    // in other words we can't use values greater than 30
    int S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = 3;

    long getS3RetryBackoffDelayMs();

    long getS3RetryBackoffMaxDelayMs();

    int getS3RetryBackoffMaxRetries();

    String getAwsS3EndPoint();

    Region getAwsS3RegionV2();

    boolean hasAwsStsRole();

    AwsBasicCredentials getAwsCredentialsV2();

    AwsCredentialsProvider getCustomCredentialsProviderV2();

    AwsStsRole getStsRole();

    String getAwsS3Prefix();

    String getAwsS3BucketName();
}
