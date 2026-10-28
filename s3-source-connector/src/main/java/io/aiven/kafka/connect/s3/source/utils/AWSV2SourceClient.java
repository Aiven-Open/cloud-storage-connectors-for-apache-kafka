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
    private final int maxKeys;
    private final boolean useSmartPrefix;
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
        this.maxKeys = s3SourceConfig.getFetchPageSize();
        this.useSmartPrefix = s3SourceConfig.useSmartPrefix();
    }

    /**
     * Creates a stream from which we will create an iterator.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return a Stream of S3Objects for the current state of the S3 storage.
     */
    public Stream<S3Object> getS3ObjectStream(final String startToken) {
        final ListObjectsV2Request request = buildInitialRequest(bucketName, s3SourceConfig.getAwsS3Prefix(),
                startToken);

        return Stream.iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
            // This is called every time next() is called on the iterator.
            if (response.isTruncated() && singleFuture == null) {
                return getResponse(response, createFutureRequest(response));
            } else if (response.isTruncated()) {
                awaitFutureCompletion();
                if (singleFuture.isCancelled()) {
                    LOGGER.warn("The greedy look ahead was cancelled");
                    return resetFutureAndReturnNull();
                }

                try {
                    submitFuture(request);
                    return getResponse(response, createFutureRequest(singleFuture.get()));
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Exception returned when using greedy look ahead", e);
                    return resetFutureAndReturnNull();
                }
            }
            return resetFutureAndReturnNull();

        }).flatMap(response -> response.contents().stream().filter(filterPredicate));
    }

    /**
     * Resets the singleFuture to null and returns a null value
     *
     * @return null
     */
    private ListObjectsV2Response resetFutureAndReturnNull() {
        singleFuture = null; // NOPMD code smell setting a value to null
        return null;
    }

    /**
     * Wait until the future has completed successfully or has been cancelled.
     */
    private void awaitFutureCompletion() {
        while (!singleFuture.isDone() && !singleFuture.isCancelled()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }

    /**
     *
     * @param response
     *            The response to be returned to
     * @param request
     *            The request to be submitted to the future executor
     * @return response
     */
    private @NonNull ListObjectsV2Response getResponse(final ListObjectsV2Response response,
            final ListObjectsV2Request request) {
        submitFuture(request);
        return response;
    }

    /**
     * Return the list of objects from the S3 bucket
     *
     * @param request
     *            The specific request built to query the S3 bucket
     * @return the response from the listObjectsV2 api with all the s3 objects
     */
    private ListObjectsV2Response getListObjectsV2Response(final ListObjectsV2Request request) {
        return s3Client.listObjectsV2(request);
    }

    /**
     * Creates a new request based off the last request that has been returned by the S3 listObjectsApi
     *
     * @param response
     *            The previous response used to determine if we can continue to build the next request from a
     *            continuation token, or if smartPrefix is enabled we build a new request to limit the scanning of the
     *            s3 bucket and improve the performance of the api.
     * @return A request that can be submitted to the S3 api
     */
    private ListObjectsV2Request createFutureRequest(final ListObjectsV2Response response) {
        if (isSmartPrefixCompatible(response) && response.hasContents() && !response.contents().isEmpty()) {
            // No start token is used with this check so that the api does not skip any slow to upload files, files
            // already processed will be skipped
            return buildInitialRequest(bucketName, response.commonPrefixes().get(0).prefix(), null);
        } else {
            return ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .maxKeys(maxKeys)
                    .prefix(response.prefix())
                    .continuationToken(response.nextContinuationToken())
                    .build();
        }
    }

    /**
     * Determines if a response is smart prefix compatible given the information it has returned. The if statement short
     * circuits and so in order the check checks If the smart prefix is configured If the response has commonPrefixes
     * set If the response has just 1 common prefix meaning that there could be more data under this prefix If the
     * common prefix returned is already the configured prefix, if it is no point in trying to be more specific with the
     * prefix If the common prefix is already set then any existing continuation tokens should be used to empty the
     * prefix of data
     *
     * @param response
     *            A response that has been returned from the S3 listObjectsV2 api
     * @return True if this response should be used to build a new request to the api , false if this response should
     *         just be used with the continuation token
     */
    private boolean isSmartPrefixCompatible(final ListObjectsV2Response response) {
        return useSmartPrefix && response.hasCommonPrefixes() && response.commonPrefixes().size() == 1
                && !Objects.equals(response.prefix(), response.commonPrefixes().get(0));
    }

    /**
     * Build a request for the S3 listObjectsV2 appi that does not contain a continuation token
     *
     * @param bucketName
     *            S3 bucket name
     * @param prefix
     *            any determined prefix to limit the scope of the bucket
     * @param startToken
     *            any S3 object key that will be used to return all data after that point
     * @return ListObjectsV2Request that can be submitted to the S3 api
     */
    private ListObjectsV2Request buildInitialRequest(final String bucketName, final String prefix,
            final String startToken) {
        return ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(maxKeys)
                .prefix(prefix)
                .startAfter(StringUtils.defaultIfBlank(startToken, null))
                .build();
    }

    /**
     * Submit a ListObjectsV2Request to the S3 api so that it can be executing in the background while the existing
     * result set is processed.
     *
     * @param request
     *            The request that is to be submitted to the API
     */
    private void submitFuture(final ListObjectsV2Request request) {
        singleFuture = lookAheadExecutorPool.submit(() -> getListObjectsV2Response(request));
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
        lookAheadExecutorPool.shutdownNow();
    }

    public void addPredicate(final Predicate<S3Object> objectPredicate) {
        this.filterPredicate = this.filterPredicate.and(objectPredicate);
    }

}
