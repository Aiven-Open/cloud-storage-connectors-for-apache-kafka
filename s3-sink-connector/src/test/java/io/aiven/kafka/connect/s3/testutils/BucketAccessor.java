/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.s3.testutils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.CompressionType;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketAccessor {

    private final String bucketName;
    private final AmazonS3 s3Client;

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketAccessor.class);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable s3Client object")
    public BucketAccessor(final AmazonS3 s3Client, final String bucketName) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    public final void createBucket() {
        s3Client.createBucket(bucketName);
    }

    public final void removeBucket() {
        final var chunk = s3Client.listObjects(bucketName)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .toArray(String[]::new);

        final var deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(chunk);
        try {
            s3Client.deleteObjects(deleteObjectsRequest);
        } catch (final MultiObjectDeleteException e) {
            for (final var err : e.getErrors()) {
                LOGGER.warn(String.format("Couldn't delete object: %s. Reason: [%s] %s", err.getKey(), err.getCode(),
                        err.getMessage()));
            }
        } catch (final AmazonClientException e) {
            LOGGER.error(
                    "Couldn't delete objects: " + Arrays.stream(chunk).reduce(" ", String::concat) + e.getMessage());
        }
        s3Client.deleteBucket(bucketName);
    }

    public final Boolean doesObjectExist(final String objectName) {
        return s3Client.doesObjectExist(bucketName, objectName);
    }

    public final List<List<String>> readAndDecodeLines(final String blobName, final CompressionType compression,
            final int... fieldsToDecode) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        return readAndDecodeLines0(blobName, compression, fieldsToDecode);
    }

    private List<List<String>> readAndDecodeLines0(final String blobName, final CompressionType compression,
            final int[] fieldsToDecode) throws IOException {
        return readLines(blobName, compression).stream()
                .map(l -> l.split(","))
                .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                .collect(Collectors.toList());
    }

    public final byte[] readBytes(final String blobName, final CompressionType compression) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        final byte[] blobBytes = s3Client.getObject(bucketName, blobName).getObjectContent().readAllBytes();
        try (InputStream decompressedStream = compression.decompress(new ByteArrayInputStream(blobBytes));
                ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
            IOUtils.copy(decompressedStream, decompressedBytes);
            return decompressedBytes.toByteArray();
        }
    }

    public final byte[] readBytes(final String blobName) throws IOException {
        return readBytes(blobName, CompressionType.NONE);
    }

    public final List<String> readLines(final String blobName, final CompressionType compression) throws IOException {
        final byte[] blobBytes = readBytes(blobName, compression);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
                InputStreamReader reader = new InputStreamReader(bais, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines().collect(Collectors.toList());
        }
    }

    public final List<String> listObjects() {
        return s3Client.listObjects(bucketName)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    private List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
