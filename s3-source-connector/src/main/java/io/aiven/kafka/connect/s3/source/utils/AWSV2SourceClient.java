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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.commons.collections.RingBuffer;
import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the AbstractSourceRecordIterator.
 */
public class AWSV2SourceClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSV2SourceClient.class);
    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final String bucketName;
    /**
     * Adding a thread executor for use with s3 listObjectsV2 api that will look ahead And already be retrieving new
     * data before we come back for more data.
     */
    public final ExecutorService lookAheadExecutorPool = Executors.newSingleThreadExecutor();

    private Predicate<S3Object> filterPredicate = s3Object -> s3Object.size() > 0;
    private Future<ListObjectsV2Response> singleFuture;

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
    public Stream<S3Object> getS3ObjectStream(final String startToken, final RingBuffer<String> ringbuffer) {
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(s3SourceConfig.getFetchPageSize())
                .prefix(s3SourceConfig.getAwsS3Prefix())
                .startAfter(StringUtils.defaultIfBlank(startToken, null))
                .build();

        return Stream.iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
            // This is called every time next() is called on the iterator.
            if (response.isTruncated() && singleFuture == null) {
                final ListObjectsV2Response resp1 = getListObjectsV2Response(response, bucketName,
                        s3SourceConfig.getFetchPageSize());
                return getResponse(response, resp1);
            } else if (singleFuture != null) {
                awaitFutureCompletion();
                if (singleFuture.isCancelled()) {
                    LOGGER.warn("The greedy look ahead was cancelled");
                    return null;
                }

                try {
                    final ListObjectsV2Response futureResponse = singleFuture.get();
                    return getResponse(response, futureResponse);
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Exception returned when using greedy look ahead", e);
                    return null;
                }
            }
            return null;

        })
                .flatMap(response -> response.contents()
                        .stream()
                        .filter(filterPredicate)
                        .filter(s3Object -> !ringbuffer.contains(s3Object.key())));
    }

    private void awaitFutureCompletion() {
        while (!singleFuture.isDone() && !singleFuture.isCancelled()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }

    private @NonNull ListObjectsV2Response getResponse(final ListObjectsV2Response response,
            final ListObjectsV2Response futureResponse) {
        if (futureResponse.isTruncated()) {
            singleFuture = lookAheadExecutorPool
                    .submit(() -> getListObjectsV2Response(response, bucketName, s3SourceConfig.getFetchPageSize()));
        }
        return futureResponse;
    }

    private ListObjectsV2Response getListObjectsV2Response(final ListObjectsV2Response response,
            final String bucketName, final int pageSize) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(pageSize)
                .continuationToken(response.nextContinuationToken())
                .build());
    }

    /**
     * Gets an iterator of keys from the current S3 storage.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return an Iterator on the keys of the current S3Objects.
     */
    public Iterator<String> getListOfObjectKeys(final String startToken, final RingBuffer<String> ringBuffer) {
        return getS3ObjectStream(startToken, ringBuffer).map(S3Object::key).iterator();
    }

    public IOSupplier<InputStream> getObject(final String objectKey) {
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();
        final ResponseBytes<GetObjectResponse> s3ObjectResponse = s3Client.getObjectAsBytes(getObjectRequest);
        return s3ObjectResponse::asInputStream;
    }

    public void shutdown() {
        s3Client.close();
        lookAheadExecutorPool.shutdownNow();
    }

    public void addPredicate(final Predicate<S3Object> objectPredicate) {
        this.filterPredicate = this.filterPredicate.and(objectPredicate);
    }

}
