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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.IteratorUtils;
import org.codehaus.plexus.util.StringUtils;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the SourceRecordIterator.
 */
public class AWSV2SourceClient {

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final AmazonS3 s3Client;
    private final String bucketName;

    private Predicate<S3ObjectSummary> filterPredicate = summary -> summary.getSize() > 0;
    private final Set<String> failedObjectKeys;

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param failedObjectKeys
     *            all objectKeys which have already been tried but have been unable to process.
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig, final Set<String> failedObjectKeys) {
        this(new S3ClientFactory().createAmazonS3Client(s3SourceConfig), s3SourceConfig, failedObjectKeys);
    }

    /**
     * Valid for testing
     *
     * @param s3Client
     *            amazonS3Client
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param failedObjectKeys
     *            all objectKeys which have already been tried but have been unable to process.
     */
    AWSV2SourceClient(final AmazonS3 s3Client, final S3SourceConfig s3SourceConfig,
            final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = s3Client;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
        this.filterPredicate = filterPredicate.and(new FailedObjectFilter());
    }

    /**
     * Gets an iterator of S3Objects. Performs the filtering based on the filters provided. Always filters Objects with
     * a size of 0 (zero), and objects that have been added to the failed objects list.
     *
     * @param startToken
     *            the token (key) to start from.
     * @return an iterator of S3Objects.
     */
    public Iterator<S3Object> getObjectIterator(final String startToken) {
        final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getS3ConfigFragment().getFetchPageSize() * PAGE_SIZE_FACTOR);
        if (StringUtils.isNotBlank(startToken)) {
            request.withStartAfter(startToken);
        }
        // perform the filtering of S3ObjectSummaries
        final Iterator<S3ObjectSummary> s3ObjectSummaryIterator = IteratorUtils
                .filteredIterator(new S3ObjectSummaryIterator(s3Client, request), filterPredicate::test);
        // transform S3ObjectSummary to S3Object
        return IteratorUtils.transformedIterator(s3ObjectSummaryIterator,
                objectSummary -> getObject(objectSummary.getKey()));
    }

    /**
     * Adds a filter by "AND"ing it to the existing filters.
     *
     * @param other
     *            the filter to add.
     */
    public void andFilter(final Predicate<S3ObjectSummary> other) {
        filterPredicate = filterPredicate.and(other);
    }

    /**
     * Adds a filter by "OR"ing it with the existing filters.
     *
     * @param other
     *            the filter to add.
     */
    public void orFilter(final Predicate<S3ObjectSummary> other) {
        filterPredicate = filterPredicate.or(other);
    }

    /**
     * Get the S3Object from the source.
     *
     * @param objectKey
     *            the object key to retrieve.
     * @return the S3Object.
     */
    public S3Object getObject(final String objectKey) {
        return s3Client.getObject(bucketName, objectKey);
    }

    /**
     * Add an object key to the list of failed keys. These will be ignored during re-reads of the data stream.
     *
     * @param objectKey
     *            the key to ignore
     */
    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    /**
     * Shuts down the system
     */
    public void shutdown() {
        s3Client.shutdown();
    }

    /**
     * Filter to remove objects that are in the failed object keys list.
     */
    class FailedObjectFilter implements Predicate<S3ObjectSummary> {
        @Override
        public boolean test(final S3ObjectSummary objectSummary) {
            return !failedObjectKeys.contains(objectSummary.getKey());
        }
    }
}
