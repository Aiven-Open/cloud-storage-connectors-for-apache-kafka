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
public class AWSV2SourceClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSV2SourceClient.class);

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final AmazonS3 s3Client;
    private final String bucketName;

    private Predicate<S3ObjectSummary> filterPredicate = summary -> summary.getSize() > 0;
    private final Set<String> failedObjectKeys;

    private final int maxTasks;
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

        int maxTasks;
        try {
            maxTasks = Integer.parseInt(s3SourceConfig.originals().get("tasks.max").toString());
        } catch (NullPointerException | NumberFormatException e) {
            LOGGER.warn("Invalid tasks.max: {}", e.getMessage());
            LOGGER.info("Setting tasks.max to 1");
            maxTasks = 1;
        }
        this.maxTasks = maxTasks;
        int taskId;
        try {
            taskId = Integer.parseInt(s3SourceConfig.originals().get("task.id").toString()) % maxTasks;
        } catch (NullPointerException | NumberFormatException e) {
            LOGGER.warn("Invalid task.id: {}", e.getMessage());
            LOGGER.info("Setting task.id to 0");
            taskId = 0;
        }
        this.taskId = taskId;
    }

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

        Predicate<S3ObjectSummary> filter = filterPredicate.and(this::checkTaskAssignment)
                .and(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey()));
        Iterator<S3ObjectSummary> summaryIterator = IteratorUtils.filteredIterator(new S3ObjectSummaryIterator(s3Client, request), filter::test);
        Iterator<S3Object> objectIterator = IteratorUtils.transformedIterator(summaryIterator, s3ObjectSummary -> s3Client.getObject(bucketName, s3ObjectSummary.getKey()));
        return ClosableIterator.wrap(objectIterator);
    }

    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    public void setFilterPredicate(final Predicate<S3ObjectSummary> predicate) {
        filterPredicate = predicate;
    }

    private boolean checkTaskAssignment(S3ObjectSummary summary) {
        return  taskId == Math.floorMod(summary.getKey().hashCode(), maxTasks);
    }
    public void shutdown() {
        s3Client.shutdown();
    }

}
