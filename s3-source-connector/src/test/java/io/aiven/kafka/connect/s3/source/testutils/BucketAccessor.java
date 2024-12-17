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

package io.aiven.kafka.connect.s3.source.testutils;

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
import java.util.zip.GZIPInputStream;

import io.aiven.kafka.connect.common.config.CompressionType;

import com.github.luben.zstd.ZstdInputStream;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class BucketAccessor {

    private final String bucketName;
    private final S3Client s3Client;

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketAccessor.class);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable s3Client object")
    public BucketAccessor(final S3Client s3Client, final String bucketName) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    public final void createBucket() {
        s3Client.createBucket(builder -> builder.bucket(bucketName).build());
    }

    public final void removeBucket() {
        final var deleteIds = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build())
                .contents()
                .stream()
                .map(S3Object::key)
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());

        try {
            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(deleteIds).build())
                    .build());
        } catch (final S3Exception e) {
            LOGGER.warn(
                    String.format("Couldn't delete objects. Reason: [%s] %s", e.awsErrorDetails().errorMessage(), e));
        } catch (final SdkException e) {

            LOGGER.error("Couldn't delete objects: {}, Exception{} ", deleteIds, e.getMessage());
        }
        s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
    }

    // TODO NOT Currently used
    public final Boolean doesObjectExist(final String objectName) {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(objectName).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public final List<List<String>> readAndDecodeLines(final String blobName, final String compression,
            final int... fieldsToDecode) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        return readAndDecodeLines0(blobName, compression, fieldsToDecode);
    }

    private List<List<String>> readAndDecodeLines0(final String blobName, final String compression,
            final int[] fieldsToDecode) throws IOException {
        return readLines(blobName, compression).stream()
                .map(l -> l.split(","))
                .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                .collect(Collectors.toList());
    }

    public final byte[] readBytes(final String blobName, final String compression) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        final byte[] blobBytes = s3Client.getObjectAsBytes(builder -> builder.key(blobName).bucket(bucketName).build())
                .asByteArray();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
                InputStream decompressedStream = getDecompressedStream(bais, compression);
                ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
            final byte[] readBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = decompressedStream.read(readBuffer)) != -1) { // NOPMD AssignmentInOperand
                decompressedBytes.write(readBuffer, 0, bytesRead);
            }
            return decompressedBytes.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e); // NOPMD AvoidThrowingRawExceptionTypes
        }
    }

    public final byte[] readBytes(final String blobName) throws IOException {
        return readBytes(blobName, "none");
    }

    public final List<String> readLines(final String blobName, final String compression) throws IOException {
        final byte[] blobBytes = readBytes(blobName, compression);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
                InputStreamReader reader = new InputStreamReader(bais, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e); // NOPMD AvoidThrowingRawExceptionTypes
        }
    }

    public final List<String> listObjects() {

        return s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build())
                .contents()
                .stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    private InputStream getDecompressedStream(final InputStream inputStream, final String compression)
            throws IOException {
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        Objects.requireNonNull(compression, "compression cannot be null");

        final CompressionType compressionType = CompressionType.forName(compression);
        switch (compressionType) {
            case ZSTD :
                return new ZstdInputStream(inputStream);
            case GZIP :
                return new GZIPInputStream(inputStream);
            case SNAPPY :
                return new SnappyInputStream(inputStream);
            default :
                return inputStream;
        }
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
