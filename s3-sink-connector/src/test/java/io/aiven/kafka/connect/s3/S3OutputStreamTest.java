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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@ExtendWith(MockitoExtension.class)
final class S3OutputStreamTest {

    static final String BUCKET_NAME = "some_bucket";

    static final String FILE_KEY = "some_key";

    static final String UPLOAD_ID = "some_upload_id";

    static final String SSEA_NAME = "AES256";

    @Mock
    S3Client mockedAmazonS3;

    @Captor
    ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;
    @Captor
    ArgumentCaptor<RequestBody> uploadBodyCaptor;

    final Random random = new Random();

    @Test
    void noRequestsForEmptyBytes() throws IOException {

        try (var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 10, mockedAmazonS3)) {
            out.write(new byte[] {});
        }

        verify(mockedAmazonS3, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedAmazonS3, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedAmazonS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedAmazonS3, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void sendsInitialAndCompletionUploadRequests() throws IOException {
        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class),any(RequestBody.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG"));
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());

        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
            outputStream.write(1);
        }

        verify(mockedAmazonS3).createMultipartUpload(createMultipartUploadRequestCaptor.capture());
        verify(mockedAmazonS3).uploadPart(any(UploadPartRequest.class) ,any(RequestBody.class));
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var initiateMultipartUploadRequest = createMultipartUploadRequestCaptor.getValue();

        assertThat(initiateMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(initiateMultipartUploadRequest.key()).isEqualTo(FILE_KEY);

//        assertThat(Integer.valueOf(initiateMultipartUploadRequest.metadata().get("ContentLanguage"))).isZero();

        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of("SOME_ETAG")
        );
    }

    @Test
    void sendsAbortForAnyExceptionWhileWriting() {
        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(AbortMultipartUploadResponse.builder().build());

        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenThrow(RuntimeException.class);

        assertThatThrownBy(() -> {
            try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
                outputStream.write(new byte[] { 1, 2, 3 });
            }
        }).isInstanceOf(IOException.class);

         verify(mockedAmazonS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
         verify(mockedAmazonS3).uploadPart(any(UploadPartRequest.class) ,any(RequestBody.class));
        verify(mockedAmazonS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendsServerSideEncryptionAlgorithmNameWhenPassed() throws IOException {
        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class) ,any(RequestBody.class)))
                .thenReturn(newUploadPartResponse( "SOME_ETAG"));

        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());

        try (var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3, SSEA_NAME)) {
            out.write(1);
        }

        verify(mockedAmazonS3).createMultipartUpload(createMultipartUploadRequestCaptor.capture());

        final var initiateMultipartUploadRequest = createMultipartUploadRequestCaptor.getValue();

        assertThat(initiateMultipartUploadRequest.sseCustomerAlgorithm()).isEqualTo(SSEA_NAME);
    }

    @Test
    void sendsAbortForAnyExceptionWhenClose() throws IOException {
        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        //AbortMultipartUploadRequest
        when(mockedAmazonS3.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(AbortMultipartUploadResponse.builder().build());

        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenThrow(RuntimeException.class);

        final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 10, mockedAmazonS3); // NOPMD CloseResource

        final var buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close).isInstanceOf(IOException.class);

        verify(mockedAmazonS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedAmazonS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void writesOneByte() throws IOException {
        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class) ,any(RequestBody.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG"));
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().build());

        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
            outputStream.write(1);
        }

        verify(mockedAmazonS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedAmazonS3).uploadPart(uploadPartRequestCaptor.capture(), uploadBodyCaptor.capture());
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        assertUploadPartRequest(
            uploadPartRequestCaptor.getValue(),
            uploadBodyCaptor.getValue(),
            1,
            1,
            new byte[] {1});
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of("SOME_ETAG")
        );
    }

    @Test
    void writesMultipleMessages() throws IOException {
        final var bufferSize = 10;
        final var message = new byte[bufferSize];

        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(a -> {
            final var uploadPartRequest = (UploadPartRequest) a.getArgument(0);
            return newUploadPartResponse("SOME_TAG#" + uploadPartRequest.partNumber());
        });
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());

        final var expectedMessagesList = new ArrayList<byte[]>();
        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedAmazonS3)) {
            for (int i = 0; i < 3; i++) {
                random.nextBytes(message);
                outputStream.write(message, 0, message.length);
                expectedMessagesList.add(message);
            }
        }

        verify(mockedAmazonS3, times(3)).uploadPart(uploadPartRequestCaptor.capture(), uploadBodyCaptor.capture());
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var uploadRequests = uploadPartRequestCaptor.getAllValues();
        final var uploadBody = uploadBodyCaptor.getAllValues();
        var counter = 0;
        for (final var expectedMessage : expectedMessagesList) {
            assertUploadPartRequest(uploadRequests.get(counter), uploadBody.get(counter), bufferSize, counter + 1,
                    expectedMessage);
            counter++;
        }
        assertCompleteMultipartUploadRequest(completeMultipartUploadRequestCaptor.getValue(),
                List.of("SOME_TAG#1", "SOME_TAG#2", "SOME_TAG#3"));
    }

    @Test
    void writesTailMessages() throws IOException {
        final var messageSize = 20;

        final var uploadPartRequests = new ArrayList<UploadPartRequest>();
        final var uploadBodyRequests = new ArrayList<RequestBody>();

        when(mockedAmazonS3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class), uploadBodyCaptor.capture())).thenAnswer(a -> {
            final var uploadPartRequest = (UploadPartRequest) a.getArgument(0);
            final var uploadBody = uploadBodyCaptor.getValue();
            // TODO check out if this part required
            // emulate behaviour of S3 client otherwise we will get wrong array in the memory

            uploadPartRequests.add(uploadPartRequest);
            uploadBodyRequests.add(uploadBody);

            return newUploadPartResponse("SOME_TAG#" + uploadPartRequest.partNumber());
        });
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());

        final var message = new byte[messageSize];

        final var expectedFullMessage = new byte[messageSize + 10];
        final var expectedTailMessage = new byte[10];

        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedAmazonS3)) {
            random.nextBytes(message);
            outputStream.write(message);
            System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
            random.nextBytes(message);
            outputStream.write(message);
            System.arraycopy(message, 0, expectedFullMessage, 20, 10);
            System.arraycopy(message, 10, expectedTailMessage, 0, 10);
        }

        assertUploadPartRequest(uploadPartRequests.get(0), uploadBodyRequests.get(0), 30, 1, expectedFullMessage);
        assertUploadPartRequest(uploadPartRequests.get(1), uploadBodyRequests.get(1), 10, 2, expectedTailMessage);

        verify(mockedAmazonS3).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedAmazonS3, times(2)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(mockedAmazonS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    private CreateMultipartUploadResponse createMultipartUploadResult() {
        return CreateMultipartUploadResponse.builder().uploadId(UPLOAD_ID).build();
    }

    private UploadPartResponse newUploadPartResponse(final String etag) {
        return UploadPartResponse.builder().eTag(etag).build();
    }

    private void assertUploadPartRequest(final UploadPartRequest uploadPartRequest, final RequestBody body,
            final int expectedPartSize, final int expectedPartNumber, final byte[] expectedBytes) throws IOException {
        assertThat(body.optionalContentLength().orElseGet(null)).isEqualTo(expectedPartSize);
        assertThat(uploadPartRequest.uploadId()).isEqualTo(UPLOAD_ID);
        assertThat(uploadPartRequest.partNumber()).isEqualTo(expectedPartNumber);
        assertThat(uploadPartRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(uploadPartRequest.key()).isEqualTo(FILE_KEY);
        assertThat(body.optionalContentLength().orElse(0L)).isEqualTo(expectedBytes.length);
    }

    private void assertCompleteMultipartUploadRequest(
            final CompleteMultipartUploadRequest completeMultipartUploadRequest, final List<String> expectedETags) {
        assertThat(completeMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(completeMultipartUploadRequest.key()).isEqualTo(FILE_KEY);
        assertThat(completeMultipartUploadRequest.uploadId()).isEqualTo(UPLOAD_ID);
        assertThat(completeMultipartUploadRequest.multipartUpload().parts()).hasSameSizeAs(expectedETags);

        assertThat(completeMultipartUploadRequest.multipartUpload()
                .parts()
                .stream()
                .map(CompletedPart::eTag)
                .collect(Collectors.toList())).isEqualTo(expectedETags);

    }

    private void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest abortMultipartUploadRequest) {
        assertThat(abortMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(abortMultipartUploadRequest.key()).isEqualTo(FILE_KEY);
        assertThat(abortMultipartUploadRequest.uploadId()).isEqualTo(UPLOAD_ID);
    }

}
