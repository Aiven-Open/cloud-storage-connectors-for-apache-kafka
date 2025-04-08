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

package io.aiven.kafka.connect.s3.source.testdata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.source.NativeInfo;

import com.github.luben.zstd.ZstdInputStream;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Utility to access access an S3 bucket
 */
public class BucketAccessor {
    /** the name of the bucket to access */
    private final String bucketName;
    /** the S3Client to access the bucket */
    private final S3Client s3Client;
    /** the logger to use */
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketAccessor.class);

    /**
     * Constructor.
     *
     * @param s3Client
     *            the S3Client to use
     * @param bucketName
     *            the bucket name to access.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable s3Client object")
    public BucketAccessor(final S3Client s3Client, final String bucketName) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    /**
     * Create the bucket.
     */
    public final void createBucket() {
        final CreateBucketResponse response = s3Client.createBucket(builder -> builder.bucket(bucketName).build());
        if (!response.sdkHttpResponse().isSuccessful()) {
            Assertions.fail("Can not create bucket: " + bucketName);
        }
    }

    /**
     * Deletes the bucket.
     */
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

    /**
     * Read bytes from the bucket
     *
     * @param blobName
     *            the name of the item to read.
     * @param compression
     *            the compresson for the item.
     * @return the byte buffer
     * @throws IOException
     *             on read error.
     */
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
        }
    }

    /**
     * Get the native list of native info for this bucket.
     *
     * @return the list of S3NativeInfo objects.
     */
    public final List<S3NativeInfo> getNativeStorage() {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build())
                .contents()
                .stream()
                .map(S3NativeInfo::new)
                .collect(Collectors.toList());
    }

    /**
     * Create a decompressed stream.
     *
     * @param inputStream
     *            the input stream to decompress
     * @param compression
     *            the compression format
     * @return a decompressed input stream
     * @throws IOException
     *             on error
     */
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

    /**
     * Implementation of NativeInfo for the S3 access.
     */
    public static final class S3NativeInfo implements NativeInfo<S3Object, String> {
        /** The S3 native object */
        private final S3Object s3Object;

        /**
         * Constructor
         *
         * @param s3Object
         *            the natvie object.
         */
        S3NativeInfo(final S3Object s3Object) {
            this.s3Object = s3Object;
        }

        @Override
        public S3Object getNativeItem() {
            return s3Object;
        }

        @Override
        public String getNativeKey() {
            return s3Object.key();
        }

        @Override
        public long getNativeItemSize() {
            return s3Object.size();
        }
    }
}
