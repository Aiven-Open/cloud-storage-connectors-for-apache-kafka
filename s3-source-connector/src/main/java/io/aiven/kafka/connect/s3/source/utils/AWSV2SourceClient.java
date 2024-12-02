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

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.FETCH_PAGE_SIZE;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.api.SourceApiClient;
import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.codehaus.plexus.util.StringUtils;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the SourceRecordIterator.
 */
public class AWSV2SourceClient implements SourceApiClient<S3Object> {

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final AmazonS3 s3Client;
    private final String bucketName;

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
    protected AWSV2SourceClient(final AmazonS3 s3Client, final S3SourceConfig s3SourceConfig,
            final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = s3Client;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
    }

    @Override
    public Iterator<String> getListOfObjects(final String startToken) {
        final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR);

        if (StringUtils.isNotBlank(startToken)) {
            request.withStartAfter(startToken);
        }

        final Stream<String> s3ObjectStream = Stream
                .iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
                    if (response.isTruncated()) {
                        return s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
                                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR)
                                .withContinuationToken(response.getNextContinuationToken()));
                    } else {
                        return null;
                    }
                })
                .flatMap(response -> response.getObjectSummaries()
                        .stream()
                        .filter(objectSummary -> objectSummary.getSize() > 0)
                        .filter(objectSummary -> assignObjectToTask(objectSummary.getKey()))
                        .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey())))
                .map(S3ObjectSummary::getKey);
        return s3ObjectStream.iterator();
    }

    @Override
    public S3Object getObject(final String objectKey) {
        return s3Client.getObject(bucketName, objectKey);
    }

    @Override
    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    private boolean assignObjectToTask(final String objectKey) {
        final int maxTasks = Integer.parseInt(s3SourceConfig.originals().get("tasks.max").toString());
        final int taskId = Integer.parseInt(s3SourceConfig.originals().get("task.id").toString()) % maxTasks;
        final int taskAssignment = Math.floorMod(objectKey.hashCode(), maxTasks);
        return taskAssignment == taskId;
    }

    public void shutdown() {
        s3Client.shutdown();
    }

}
