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

package io.aiven.kafka.connect.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

//import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.aiven.kafka.connect.config.s3.S3Config;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3OutputStream extends OutputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3OutputStream.class);

    private final ByteBuffer byteBuffer;

    private final MultipartUpload multipartUpload;

    private volatile boolean closed;


    //@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "AmazonS3 client is mutable")
    public S3OutputStream(final S3SinkConfig config, final String key, final S3Client client) {
        this(config.getAwsS3PartSize(), new MultipartUpload(config, key, client));
    }

    /* package private for testing */
    S3OutputStream(final int partSize, MultipartUpload multipartUpload) {
        super();
        this.byteBuffer = ByteBuffer.allocate(partSize);
        this.multipartUpload = multipartUpload;
        closed = multipartUpload == null;
    }

    private void ensureOpen() throws IOException {
        if (closed ) {
            throw new IOException("Stream closed");
        }
    }


    @Override
    public void write(final int singleByte) throws IOException {
        write(new byte[] { (byte) singleByte }, 0, 1);
    }

    @Override
    public void write(final byte[] bytes, final int off, final int len) throws IOException {
        ensureOpen();
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return;
        }
        final var source = ByteBuffer.wrap(bytes, off, len);
        while (source.hasRemaining()) {
            final int transferred = Math.min(byteBuffer.remaining(), source.remaining());
            final int offset = source.arrayOffset() + source.position();
            byteBuffer.put(source.array(), offset, transferred);
            source.position(source.position() + transferred);
            if (!byteBuffer.hasRemaining()) {
                flush();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        if (byteBuffer.position() > 0) {
            try {
                multipartUpload.uploadPart(byteBuffer.flip());
                byteBuffer.clear();
            } catch (AwsServiceException | SdkClientException exception) {
                LOGGER.error("Unable to write to S3 -- aborting", exception);
                abort(exception);
            }
        }
    }


    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        flush();
        try {
            multipartUpload.complete();
        } catch (AwsServiceException | SdkClientException exception) {
            LOGGER.error("Unable to write to S3 -- aborting", exception);
            abort(exception);
        }
        closed = true;
        super.close();
    }

    private void abort(RuntimeException exception) throws IOException {
        multipartUpload.abort();
        closed = true;
        throw new IOException(exception);
    }

    // package private for testing
    static class MultipartUpload {
        private final S3Client client;
        private final CreateMultipartUploadResponse response;
        private int partCount = 0;


        private MultipartUpload(final S3SinkConfig config, String key, S3Client client) throws software.amazon.awssdk.awscore.exception.AwsServiceException, software.amazon.awssdk.core.exception.SdkClientException {
            S3OutputStream.LOGGER.debug("Creating new multipart upload request");
            this.client = client;
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(config.getAwsS3BucketName()).key(key)
                    .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                    .serverSideEncryption(config.getServerSideEncryptionAlgorithmName()).build();
            response = client.createMultipartUpload(createMultipartUploadRequest);
            LOGGER.debug("Upload ID: {}", response.uploadId());
        }

        public UploadPartResponse uploadPart(ByteBuffer byteBuffer) throws software.amazon.awssdk.awscore.exception.AwsServiceException, software.amazon.awssdk.core.exception.SdkClientException {
            final UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(response.bucket())
                    .key(response.key())
                    .uploadId(response.uploadId())
                    .checksumAlgorithm(response.checksumAlgorithm())
                    .partNumber(partCount++)
                    .contentLength( (long) byteBuffer.position())
                    .build();

            RequestBody body = RequestBody.fromByteBuffer(byteBuffer);
            return client.uploadPart(uploadPartRequest, body);
        }

        public CompleteMultipartUploadResponse complete() throws software.amazon.awssdk.awscore.exception.AwsServiceException, software.amazon.awssdk.core.exception.SdkClientException {
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(response.bucket()).key(response.key())
                    .uploadId(response.uploadId())
                            .build();
            return client.completeMultipartUpload(completeMultipartUploadRequest);
        }

        public AbortMultipartUploadResponse abort() throws software.amazon.awssdk.awscore.exception.AwsServiceException, software.amazon.awssdk.core.exception.SdkClientException {
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(response.bucket()).key(response.key())
                    .uploadId(response.uploadId())
                            .build();
            return client.abortMultipartUpload(abortMultipartUploadRequest);
        }
    }

}
