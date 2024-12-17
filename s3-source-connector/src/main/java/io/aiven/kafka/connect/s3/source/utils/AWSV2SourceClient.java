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
import org.codehaus.plexus.util.StringUtils;
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

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final String bucketName;

    private Predicate<S3Object> filterPredicate = s3Object -> s3Object.size() > 0;
    private final Set<String> failedObjectKeys;

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param failedObjectKeys
     *            all objectKeys which have already been tried but have been unable to process.
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig, final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        final S3ClientFactory s3ClientFactory = new S3ClientFactory();
        this.s3Client = s3ClientFactory.createAmazonS3Client(s3SourceConfig);
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
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
    }

    public Iterator<String> getListOfObjectKeys(final String startToken) {
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(s3SourceConfig.getS3ConfigFragment().getFetchPageSize() * PAGE_SIZE_FACTOR)
                .prefix(optionalKey(s3SourceConfig.getAwsS3Prefix()))
                .startAfter(optionalKey(startToken))
                .build();

        final Stream<String> s3ObjectKeyStream = Stream
                .iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
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
                        .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.key())))
                .map(S3Object::key);
        return s3ObjectKeyStream.iterator();
    }
    private String optionalKey(final String key) {
        if (StringUtils.isNotBlank(key)) {
            return key;
        }
        return null;
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
        final int maxTasks = Integer.parseInt(s3SourceConfig.originals().get("tasks.max").toString());
        final int taskId = Integer.parseInt(s3SourceConfig.originals().get("task.id").toString()) % maxTasks;
        final int taskAssignment = Math.floorMod(objectKey.hashCode(), maxTasks);
        return taskAssignment == taskId;
    }

    public void shutdown() {
        s3Client.close();
    }

}
