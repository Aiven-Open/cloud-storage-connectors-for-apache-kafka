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

import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the SourceRecordIterator.
 */
public class AWSV2SourceClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSV2SourceClient.class);
    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final String bucketName;

    private Predicate<S3Object> filterPredicate = s3Object -> s3Object.size() > 0;
    private final Set<String> failedObjectKeys;

    private final int taskId;
    private final int maxTasks;

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
    AWSV2SourceClient(final S3Client s3Client, final S3SourceConfig s3SourceConfig,
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
     * Creates a stream from which we will create an iterator.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return a Stream of S3Objects for the current state of the S3 storage.
     */
    private Stream<S3Object> getS3ObjectStream(final String startToken) {
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(s3SourceConfig.getS3ConfigFragment().getFetchPageSize() * PAGE_SIZE_FACTOR)
                .prefix(StringUtils.defaultIfBlank(s3SourceConfig.getAwsS3Prefix(), null))
                .startAfter(StringUtils.defaultIfBlank(startToken, null))
                .build();

        return Stream.iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
            // This is called every time next() is called on the iterator.
            if (response.isTruncated()) {
                return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .maxKeys(s3SourceConfig.getS3ConfigFragment().getFetchPageSize() * PAGE_SIZE_FACTOR)
                        .continuationToken(response.nextContinuationToken())
                        .build());
            } else {
                return null;
            }

        })
                .flatMap(response -> response.contents()
                        .stream()
                        .filter(filterPredicate)
                        .filter(objectSummary -> assignObjectToTask(objectSummary.key()))
                        .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.key())));
    }

    /**
     * Creates an S3Object iterator that will return the objects from the current objects in S3 storage and then try to
     * refresh on every {@code hasNext()} that returns false. This should pick up new files as they are dropped on the
     * file system.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return an Iterator on the S3Objects.
     */
    public Iterator<S3Object> getS3ObjectIterator(final String startToken) {
        return new S3ObjectIterator(startToken);
    }

    /**
     * Gets an iterator of keys from the current S3 storage.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return an Iterator on the keys of the current S3Objects.
     */
    public Iterator<String> getListOfObjectKeys(final String startToken) {
        return getS3ObjectStream(startToken).map(S3Object::key).iterator();
    }

    public IOSupplier<InputStream> getObject(final String objectKey) {
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();
        final ResponseBytes<GetObjectResponse> s3ObjectResponse = s3Client.getObjectAsBytes(getObjectRequest);
        return s3ObjectResponse::asInputStream;
    }

    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    public void setFilterPredicate(final Predicate<S3Object> predicate) {
        filterPredicate = predicate;
    }

    private boolean assignObjectToTask(final String objectKey) {
        final int taskAssignment = Math.floorMod(objectKey.hashCode(), maxTasks);
        return taskAssignment == taskId;
    }

    public void shutdown() {
        s3Client.close();
    }

    /**
     * An iterator that reads from
     */
    public class S3ObjectIterator implements Iterator<S3Object> {

        /** The current iterator. */
        private Iterator<S3Object> inner;
        /** The last object key that was seen. */
        private String lastSeenObjectKey;

        private S3ObjectIterator(final String initialKey) {
            lastSeenObjectKey = initialKey;
            inner = getS3ObjectStream(lastSeenObjectKey).iterator();
        }
        @Override
        public boolean hasNext() {
            if (!inner.hasNext()) {
                inner = getS3ObjectStream(lastSeenObjectKey).iterator();
            }
            return inner.hasNext();
        }

        @Override
        public S3Object next() {
            final S3Object result = inner.next();
            lastSeenObjectKey = result.key();
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
