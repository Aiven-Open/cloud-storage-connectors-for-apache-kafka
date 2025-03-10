package io.aiven.kafka.connect.config.s3;
/*
This class is based on code  from com.amazonaws.services.s3.internal.BucketNameValidator
with modifications for use within AWS V2 client.
 */
/*
 * Copyright 2010-2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Utilities for working with Amazon S3 bucket names, such as validation and
 * checked to see if they are compatible with DNS addressing.
 */
public class BucketNameValidator implements ConfigDef.Validator {
    private static final int MIN_BUCKET_NAME_LENGTH = 3;
    private static final int MAX_BUCKET_NAME_LENGTH = 63;

    private static final Pattern ipAddressPattern = Pattern.compile("(\\d+\\.){3}\\d+");


    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            isValidV2BucketName((String) value).ifPresent(msg -> {
                throw new ConfigException("Illegal bucket name: " + msg);
            });
        }
    }

    /**
     * Validate whether the given input is a valid bucket name. If throwOnError
     * is true, throw an IllegalArgumentException if validation fails. If
     * false, simply return 'false'.
     *
     * @param bucketName the name of the bucket
     * @return Optional error message or empty if no issue.
     */
    public Optional<String> isValidV2BucketName(final String bucketName) {

        if (bucketName == null) {
            return Optional.of("Bucket name cannot be null");
        }

        if (bucketName.length() < MIN_BUCKET_NAME_LENGTH ||
                bucketName.length() > MAX_BUCKET_NAME_LENGTH) {

            return Optional.of("Bucket name should be between " + MIN_BUCKET_NAME_LENGTH + " and " + MAX_BUCKET_NAME_LENGTH +" characters long"
            );
        }

        if (ipAddressPattern.matcher(bucketName).matches()) {
            return Optional.of("Bucket name must not be formatted as an IP Address"
            );
        }

        char previous = '\0';

        for (int i = 0; i < bucketName.length(); ++i) {
            char next = bucketName.charAt(i);

            if (next >= 'A' && next <= 'Z') {
                return Optional.of("Bucket name should not contain uppercase characters"
                );
            }

            if (next == ' ' || next == '\t' || next == '\r' || next == '\n') {
                return Optional.of("Bucket name should not contain white space"
                );
            }

            if (next == '.') {
                if (previous == '\0') {
                    return Optional.of("Bucket name should not begin with a period"
                    );
                }
                if (previous == '.') {
                    return Optional.of("Bucket name should not contain two adjacent periods"
                    );
                }
                if (previous == '-') {
                    return Optional.of("Bucket name should not contain dashes next to periods"
                    );
                }
            } else if (next == '-') {
                if (previous == '.') {
                    return Optional.of("Bucket name should not contain dashes next to periods"
                    );
                }
                if (previous == '\0') {
                    return Optional.of("Bucket name should not begin with a '-'"
                    );
                }
            } else if ((next < '0')
                    || (next > '9' && next < 'a')
                    || (next > 'z')) {

                return Optional.of("Bucket name should not contain '" + next + "'"
                );
            }

            previous = next;
        }

        if (previous == '.' || previous == '-') {
            return Optional.of("Bucket name should not end with '-' or '.'"
            );
        }

        return Optional.empty();
    }
}
