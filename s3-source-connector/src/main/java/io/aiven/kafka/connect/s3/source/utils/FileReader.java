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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.IteratorUtils;

/**
 * Class to read the S3 connection and return lists of object summaries.
 * Package private so that is it not a public interface.
 */
public class FileReader {

    public static final int PAGE_SIZE_FACTOR = 2;
    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private final Set<String> failedObjectKeys;

    /**
     * Concstructs the File reader
     * @param s3SourceConfig the S3Source configuration to use to access S3.
     * @param bucketName the bucket name to retrive
     * @param failedObjectKeys The set of failed object keys.
     */
    public FileReader(final S3SourceConfig s3SourceConfig, final String bucketName,
            final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.bucketName = bucketName;
        this.failedObjectKeys = new HashSet<>(failedObjectKeys);
    }

    /**
     * Creates an iterator over the files in S3.
     * @param s3Client The client.
     * @return An iterator.
     */
    Iterator<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) {
        final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR);

        // the predicate to filter the results of the base iterator.  Additional filters can be applied with Predicate.or and/or Predicate.and
        // this predicate removes empty document and any that are already listed in the failedObjectKeys
        final Predicate<S3ObjectSummary> filter = objectSummary -> objectSummary.getSize() > 0 && !failedObjectKeys.contains(objectSummary.getKey());

        final S3ObjectSummaryIterator s3ObjectSummaryIterator = new S3ObjectSummaryIterator(s3Client, request);

        return IteratorUtils.filteredIterator(s3ObjectSummaryIterator, s -> filter.test(s));
    }

}
