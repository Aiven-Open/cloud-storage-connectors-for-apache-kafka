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
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the AbstractSourceRecordIterator.
 */
public class AWSV2SourceClient {

    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final String bucketName;

    private Predicate<S3Object> filterPredicate = s3Object -> s3Object.size() > 0;

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig) {
        this(new S3ClientFactory().createAmazonS3Client(s3SourceConfig), s3SourceConfig);
    }

    /**
     * Valid for testing
     *
     * @param s3Client
     *            amazonS3Client
     * @param s3SourceConfig
     *            configuration for Source connector
     */
    AWSV2SourceClient(final S3Client s3Client, final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = s3Client;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
    }

    /**
     * Creates a stream from which we will create an iterator.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return a Stream of S3Objects for the current state of the S3 storage.
     */
    public Stream<S3Object> getS3ObjectStream(final String startToken) {
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(s3SourceConfig.getFetchPageSize())
                .prefix(s3SourceConfig.getAwsS3Prefix())
                .startAfter(StringUtils.defaultIfBlank(startToken, null))
                .build();

        return Stream.iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
            // This is called every time next() is called on the iterator.
            if (response.isTruncated()) {
                return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .maxKeys(s3SourceConfig.getFetchPageSize())
                        .continuationToken(response.nextContinuationToken())
                        .build());
            } else {
                return null;
            }

        }).flatMap(response -> response.contents().stream().filter(filterPredicate));
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

    public void shutdown() {
        s3Client.close();
    }

    public void addPredicate(final Predicate<S3Object> objectPredicate) {
        this.filterPredicate = this.filterPredicate.and(objectPredicate);
    }

}
