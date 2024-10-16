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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
    private final Set<String> inProcessObjectKeys;

    public FileReader(final S3SourceConfig s3SourceConfig, final String bucketName, final Set<String> failedObjectKeys,
            final Set<String> inProcessObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.bucketName = bucketName;
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
        this.inProcessObjectKeys = new HashSet<>(inProcessObjectKeys);
    }

    public Set<String> getInProcessObjectKeys() {
        return Collections.unmodifiableSet(this.inProcessObjectKeys);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    Iterator<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) throws IOException {
        final List<S3ObjectSummary> allSummaries = new ArrayList<>();
        String continuationToken = null;
        ListObjectsV2Result objectListing;

        do {
            // Create the request for listing objects
            final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                    .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR)
                    .withContinuationToken(continuationToken); // Set continuation token for pagination

            // List objects from S3
            objectListing = s3Client.listObjectsV2(request);

            // Filter out zero-byte objects and add to the list
            final List<S3ObjectSummary> filteredSummaries = objectListing.getObjectSummaries()
                    .stream()
                    .filter(objectSummary -> objectSummary.getSize() > 0)
                    .filter(objectSummary -> !failedObjectKeys.contains(objectSummary.getKey()))
                    .filter(objectSummary -> !inProcessObjectKeys.contains(objectSummary.getKey()))
                    .collect(Collectors.toList());

            allSummaries.addAll(filteredSummaries); // Add the filtered summaries to the main list

            // TO BE REMOVED before release
            allSummaries.forEach(objSummary -> LOGGER.info("Objects to be processed {} ", objSummary.getKey()));

            // Objects being processed
            allSummaries.forEach(objSummary -> this.inProcessObjectKeys.add(objSummary.getKey()));

            // Check if there are more objects to fetch
            continuationToken = objectListing.getNextContinuationToken();
        } while (objectListing.isTruncated()); // Continue fetching if the result is truncated

        return allSummaries.iterator();
    }

    public void addFailedObjectKeys(final String objectKey) {
        this.failedObjectKeys.add(objectKey);
    }

    public void removeProcessedObjectKeys(final String objectKey) {
        this.inProcessObjectKeys.remove(objectKey);
    }
}
