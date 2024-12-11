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

package io.aiven.kafka.connect.s3.source.testutils;

import static org.mockito.Mockito.when;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Standard utilities to create objects from S3 for testing.
 */
public final class S3ObjectsUtils {

    private S3ObjectsUtils() {
        // do not instantiate.
    }

    /**
     * Create a ListObjectV2Result from a list of summaries and an next token.
     *
     * @param summaries
     *            the list of object summaries to create the result from.
     * @param nextToken
     *            the next token (may be {@code null}).
     * @return the ListObjectV2Result from a list of summaries and an next token.
     */
    public static ListObjectsV2Result createListObjectsV2Result(final List<S3ObjectSummary> summaries,
            final String nextToken) {
        final ListObjectsV2Result result = new ListObjectsV2Result() {
            @Override
            public List<S3ObjectSummary> getObjectSummaries() {
                return summaries;
            }
        };
        result.setContinuationToken(nextToken);
        result.setTruncated(nextToken != null);
        return result;
    }

    /**
     * Creates an object summary with the specified key. The summary will have a size of 1.
     *
     * @param bucket
     *            the bucket name.
     * @param objectKey
     *            the key to create the summary for.
     * @return an object summary with the specified key..
     */
    public static S3ObjectSummary createObjectSummary(final String bucket, final String objectKey) {
        return createObjectSummary(1, bucket, objectKey);
    }

    /**
     * Create an S3ObjectSummary with the specified size and object key.
     *
     * @param sizeOfObject
     *            the size for the object summary.
     * @param bucket
     *            the bucket name
     * @param objectKey
     *            the key for the object summary.
     * @return an S3ObjectSummary with the specified size and object key.
     */
    public static S3ObjectSummary createObjectSummary(final long sizeOfObject, final String bucket,
            final String objectKey) {
        final S3ObjectSummary summary = new S3ObjectSummary();
        summary.setSize(sizeOfObject);
        summary.setKey(objectKey);
        summary.setBucketName(bucket);
        return summary;
    }

    /**
     * Create an S3Object for a key.
     *
     * @param bucket
     *            the bucket for the object.
     * @param key
     *            the key to create the object for
     * @return the S3Object.
     */
    public static S3Object createS3Object(final String bucket, final String key) {
        return createS3Object(createObjectSummary(bucket, key));
    }

    /**
     * Creates an S43Object from the object summary.
     *
     * @param summary
     *            the object summary.
     * @return the S3Object for the summary
     */
    public static S3Object createS3Object(final S3ObjectSummary summary) {
        final S3Object s3Object = new S3Object();
        s3Object.setKey(summary.getKey());
        s3Object.setBucketName(summary.getBucketName());
        return s3Object;
    }

    /**
     * Add the result the S3Client so that it will return them.
     *
     * @param s3Client
     *            the mock S3Client to add result to.
     * @param result
     *            the list of S3ObjectSummary objects to place in the S3Client.
     */
    public static void populateS3Client(final AmazonS3 s3Client, final ListObjectsV2Result result) {
        for (final S3ObjectSummary summary : result.getObjectSummaries()) {
            when(s3Client.getObject(summary.getBucketName(), summary.getKey())).thenReturn(createS3Object(summary));
        }
    }
}
