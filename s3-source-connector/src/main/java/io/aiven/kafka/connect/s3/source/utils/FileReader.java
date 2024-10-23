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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);
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

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    Iterator<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) {
        return new Iterator<>() {
            private String continuationToken = null; // NOPMD
            private List<S3ObjectSummary> currentBatch = new ArrayList<>();
            private int currentIndex = 0; // NOPMD
            private boolean isTruncated = true;

            @Override
            public boolean hasNext() {
                // If there are unprocessed objects in the current batch, we return true
                if (currentIndex < currentBatch.size()) {
                    return true;
                }

                if (isTruncated) {
                    fetchNextBatch();
                    return !currentBatch.isEmpty();
                }

                return false;
            }

            @Override
            public S3ObjectSummary next() {
                if (!hasNext()) {
                    return null;
                }

                return currentBatch.get(currentIndex++);
            }

            private void fetchNextBatch() {
                currentBatch.clear();
                currentIndex = 0;

                final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                        .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR)
                        .withContinuationToken(continuationToken);

                final ListObjectsV2Result objectListing = s3Client.listObjectsV2(request);
                currentBatch = objectListing.getObjectSummaries()
                        .stream()
                        .filter(objectSummary -> objectSummary.getSize() > 0)
                        .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey()))
                        .collect(Collectors.toList());

                continuationToken = objectListing.getNextContinuationToken();
                isTruncated = objectListing.isTruncated();

                currentBatch.forEach(objSummary -> LOGGER.debug("Objects to be processed {} ", objSummary.getKey()));
            }
        };
    }
    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }
}
