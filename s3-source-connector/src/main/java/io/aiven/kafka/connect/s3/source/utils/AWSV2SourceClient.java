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

import io.aiven.kafka.connect.common.ClosableIterator;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.IteratorUtils;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the SourceRecordIterator.
 */
public final class AWSV2SourceClient {
    /** The logger to use */
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSV2SourceClient.class);
    /**
     * How many pages of data we will attempt to read at one go. The page size is defined in
     * {@link S3ConfigFragment#getFetchPageSize()}
     */
    public static final int PAGE_SIZE_FACTOR = 2;
    /** The source configuration */
    private final S3SourceConfig s3SourceConfig;
    /** The Amazon S3 client we use */
    private final AmazonS3 s3Client;
    /** The bucket we ar ereading from. */
    private final String bucketName;
    /** The predicate to filter S3ObjectSummaries */
    private Predicate<S3ObjectSummary> filterPredicate = summary -> summary.getSize() > 0;
    /** The set of failed object keys */
    private final Set<String> failedObjectKeys;
    /** The maximum number of tasks that may be executing */
    private final int maxTasks;
    /** Our taks number */
    private final int taskId;

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
     * Constructor.
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

        // TODO the code below should be configured in some sort of taks assignement method/process/call.
        int maxTasks;
        try {
            final Object value = s3SourceConfig.originals().get("tasks.max");
            if (value == null) {
                LOGGER.info("Setting tasks.max to 1");
                maxTasks = 1;
            } else {
                maxTasks = Integer.parseInt(value.toString());
            }
        } catch (NumberFormatException e) { // NOPMD catch null pointer
            LOGGER.warn("Invalid tasks.max: {}", e.getMessage());
            LOGGER.info("Setting tasks.max to 1");
            maxTasks = 1;
        }
        this.maxTasks = maxTasks;
        int taskId;
        try {
            final Object value = s3SourceConfig.originals().get("task.id");
            if (value == null) {
                LOGGER.info("Setting task.id to 0");
                taskId = 0;
            } else {
                taskId = Integer.parseInt(value.toString()) % maxTasks;
            }
        } catch (NumberFormatException e) { // NOPMD catch null pointer
            LOGGER.warn("Invalid task.id: {}", e.getMessage());
            LOGGER.info("Setting task.id to 0");
            taskId = 0;
        }
        this.taskId = taskId;
    }

    /**
     * Create an iterator of S3Objects. The iterator will automatically close the objects when the next object is
     * retrieved or when the end of the iterator is reached.
     *
     * @param startToken
     *            the Key to start searching from.
     * @return An iterator on S3Objects.
     */
    public Iterator<S3Object> getIteratorOfObjects(final String startToken) {

        final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getS3ConfigFragment().getFetchPageSize() * PAGE_SIZE_FACTOR);

        if (StringUtils.isNotBlank(startToken)) {
            request.withStartAfter(startToken);
        }
        // Prefix is optional so only use if supplied
        if (StringUtils.isNotBlank(s3SourceConfig.getAwsS3Prefix())) {
            request.withPrefix(s3SourceConfig.getAwsS3Prefix());
        }

        final Predicate<S3ObjectSummary> filter = filterPredicate.and(this::checkTaskAssignment)
                .and(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey()));
        final Iterator<S3ObjectSummary> summaryIterator = IteratorUtils
                .filteredIterator(new S3ObjectSummaryIterator(s3Client, request), filter::test);
        final Iterator<S3Object> objectIterator = IteratorUtils.transformedIterator(summaryIterator,
                s3ObjectSummary -> s3Client.getObject(bucketName, s3ObjectSummary.getKey()));
        return ClosableIterator.wrap(objectIterator);
    }

    /**
     * Add a failed object to the list of failed object keys.
     *
     * @param objectKey
     *            the object key that failed.
     */
    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    /**
     * Set the filter predicate. Overrides the default predicate.
     *
     * @param predicate
     *            the predicate to use instead of the default predicate.
     */
    public void setFilterPredicate(final Predicate<S3ObjectSummary> predicate) {
        filterPredicate = predicate;
    }

    /**
     * Checks the task assignment. This method should probalby be delivered by a task assignment object.
     *
     * @param summary
     *            the object summary to check
     * @return {@code true} if the summary should be processed, {@code false otherwise}.
     */
    private boolean checkTaskAssignment(final S3ObjectSummary summary) {
        return taskId == Math.floorMod(summary.getKey().hashCode(), maxTasks);
    }

    /**
     * Shut down this source client. Shuts down the attached AmazonS3 client.
     */
    public void shutdown() {
        s3Client.shutdown();
    }
}
