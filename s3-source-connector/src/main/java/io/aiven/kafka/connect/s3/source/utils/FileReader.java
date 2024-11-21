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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.FETCH_PAGE_SIZE;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class FileReader {

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private final Set<String> failedObjectKeys;

    public FileReader(final S3SourceConfig s3SourceConfig, final String bucketName,
            final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.bucketName = bucketName;
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
    }

    Iterator<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) {
        final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR);

        final Stream<S3ObjectSummary> s3ObjectStream = Stream
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
                        .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey())));
        return s3ObjectStream.iterator();
    }

    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    private boolean assignObjectToTask(final String objectKey) {
        final int maxTasks = Integer.parseInt(s3SourceConfig.originals().get("tasks.max").toString());
        final int taskId = Integer.parseInt(s3SourceConfig.originals().get("task.id").toString()) % maxTasks;
        final int taskAssignment = Math.floorMod(objectKey.hashCode(), maxTasks);
        return taskAssignment == taskId;
    }
}
