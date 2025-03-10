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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;


import io.aiven.kafka.connect.s3.config.S3SinkConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(MockitoExtension.class)
final class S3OutputStreamTest {

    static final String BUCKET_NAME = "some_bucket";

    static final String FILE_KEY = "some_key";

    static final String UPLOAD_ID = "some_upload_id";

    static final String SSEA_NAME = "AES256";

    @Mock
    S3Client s3Client;

    @Mock
    S3SinkConfig config;

    @Captor
    ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;

    final Random random = new Random();

    private void setupConfig(int bufferSize) {
        when(config.getAwsS3PartSize()).thenReturn(bufferSize);
        when(config.getAwsS3BucketName()).thenReturn(BUCKET_NAME);
        when(config.getServerSideEncryptionAlgorithmName()).thenReturn(SSEA_NAME);
    }

    private static void setupCreateMultipartUploadRequest(S3Client s3Client) {
        when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .then(invocation -> {
                    CreateMultipartUploadRequest req = invocation.getArgument(0, CreateMultipartUploadRequest.class);
                    return CreateMultipartUploadResponse.builder().uploadId(UPLOAD_ID).bucket(req.bucket()).key(req.key()).build();
                });
    }

    private static void setupUploadPart(S3Client s3Client) {
        when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenReturn(UploadPartResponse.builder().eTag("SOME_ETAG").build());
    }

    private static void setupCompleteMultipartUploadRequest(S3Client s3Client) {
        when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());
    }

    @Test
    void noRequestsForEmptyBytes() throws IOException {
        setupConfig(10);

        try (var out = new S3OutputStream(config, FILE_KEY, s3Client)) {
            out.write(new byte[] {});
        }

        verify(s3Client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3Client, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3Client, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void sendsInitialAndCompletionUploadRequests() throws IOException {
        setupConfig(100);
        setupCreateMultipartUploadRequest(s3Client);
        setupUploadPart(s3Client);
        setupCompleteMultipartUploadRequest(s3Client);

        try (S3OutputStream outputStream = new S3OutputStream(config, FILE_KEY, s3Client)) {
            outputStream.write(1);
        }

        verify(s3Client).createMultipartUpload(createMultipartUploadRequestCaptor.capture());
        verify(s3Client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var createMultipartUploadRequest = createMultipartUploadRequestCaptor.getValue();

        assertThat(createMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(createMultipartUploadRequest.key()).isEqualTo(FILE_KEY);
        assertCompleteMultipartUploadRequest(completeMultipartUploadRequestCaptor.getValue());
    }

    @ParameterizedTest( name = "{index} {0}")
    @MethodSource("abortTestData")
    void sendsAbortForAnyExceptionWriting(String name, S3Client s3ClientArg, int createTimes, int uploadTimes, int completeTimes, boolean aborted, Class<?> exceptionClass) throws IOException {
        setupConfig(100);
        assertThatThrownBy(() -> {
            try (var out = new S3OutputStream(config, FILE_KEY, s3ClientArg)) {
                out.write(new byte[] {1, 2, 3});
            }
        }).isInstanceOf(exceptionClass);

        verify(s3Client, times(createTimes)).createMultipartUpload(createMultipartUploadRequestCaptor.capture());
        verify(s3Client, times(uploadTimes)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, times(completeTimes)).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
        verify(s3Client, times(aborted ? 1 : 0)).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
    }

    static Stream<Arguments> abortTestData() {
        List<Arguments> lst = new ArrayList<>();

        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenThrow(AwsServiceException.class);
        setupUploadPart(client);
        setupCompleteMultipartUploadRequest(client);
        lst.add(Arguments.of("badCreate", client, 0, 0, 0, false, AwsServiceException.class));

        client = mock(S3Client.class);
        setupCreateMultipartUploadRequest(client);
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenThrow(AwsServiceException.class);
        setupCompleteMultipartUploadRequest(client);q
        lst.add(Arguments.of("badUpload", client, 1, 0, 0, true, IOException.class));

        client = mock(S3Client.class);
        setupCreateMultipartUploadRequest(client);
        setupUploadPart(client);
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(AwsServiceException.class);
        lst.add(Arguments.of("badUpload", client, 1, 1, 0, true, IOException.class));

        return lst.stream();
    }
//
//    @Test
//    void sendsServerSideEncryptionAlgorithmNameWhenPassed() throws IOException {
//        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
//                .thenReturn(newInitiateMultipartUploadResult());
//        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
//                .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
//        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
//                .thenReturn(new CompleteMultipartUploadResult());
//
//        try (var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3, SSEA_NAME)) {
//            out.write(1);
//        }
//
//        verify(mockedAmazonS3).initiateMultipartUpload(createMultipartUploadRequestCaptor.capture());
//
//        final var initiateMultipartUploadRequest = createMultipartUploadRequestCaptor.getValue();
//
//        assertThat(initiateMultipartUploadRequest.getObjectMetadata().getSSEAlgorithm()).isEqualTo(SSEA_NAME);
//    }
//
//    @Test
//    void sendsAbortForAnyExceptionWhenClose() throws IOException {
//        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
//            .thenReturn(newInitiateMultipartUploadResult());
//        doNothing().when(mockedAmazonS3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
//
//        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
//            .thenThrow(RuntimeException.class);
//
//        final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 10, mockedAmazonS3); // NOPMD CloseResource
//
//        final var buffer = new byte[5];
//        random.nextBytes(buffer);
//        out.write(buffer, 0, buffer.length);
//
//        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
//
//        verify(mockedAmazonS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
//        verify(mockedAmazonS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
//
//        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
//    }
//
//    @Test
//    void writesOneByte() throws IOException {
//        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
//            .thenReturn(newInitiateMultipartUploadResult());
//        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
//            .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
//        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
//            .thenReturn(new CompleteMultipartUploadResult());
//
//        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
//            outputStream.write(1);
//        }
//
//        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
//        verify(mockedAmazonS3).uploadPart(uploadPartRequestCaptor.capture());
//        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
//
//        assertUploadPartRequest(
//            uploadPartRequestCaptor.getValue(),
//            1,
//            1,
//            new byte[] {1});
//        assertCompleteMultipartUploadRequest(
//            completeMultipartUploadRequestCaptor.getValue(),
//            List.of(new PartETag(1, "SOME_ETAG"))
//        );
//    }
//
//    @Test
//    void writesMultipleMessages() throws IOException {
//        final var bufferSize = 10;
//        final var message = new byte[bufferSize];
//
//        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
//                .thenReturn(newInitiateMultipartUploadResult());
//        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class))).thenAnswer(a -> {
//            final var uploadPartRequest = (UploadPartRequest) a.getArgument(0);
//            return newUploadPartResult(uploadPartRequest.getPartNumber(),
//                    "SOME_TAG#" + uploadPartRequest.getPartNumber());
//        });
//        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
//                .thenReturn(new CompleteMultipartUploadResult());
//
//        final var expectedMessagesList = new ArrayList<byte[]>();
//        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedAmazonS3)) {
//            for (int i = 0; i < 3; i++) {
//                random.nextBytes(message);
//                outputStream.write(message, 0, message.length);
//                expectedMessagesList.add(message);
//            }
//        }
//
//        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
//        verify(mockedAmazonS3, times(3)).uploadPart(uploadPartRequestCaptor.capture());
//        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
//
//        final var uploadRequests = uploadPartRequestCaptor.getAllValues();
//        var counter = 0;
//        for (final var expectedMessage : expectedMessagesList) {
//            assertUploadPartRequest(uploadRequests.get(counter), bufferSize, counter + 1, expectedMessage);
//            counter++;
//        }
//        assertCompleteMultipartUploadRequest(completeMultipartUploadRequestCaptor.getValue(),
//                List.of(new PartETag(1, "SOME_TAG#1"), new PartETag(2, "SOME_TAG#2"), new PartETag(3, "SOME_TAG#3")));
//    }
//
//    @Test
//    void writesTailMessages() throws IOException {
//        final var messageSize = 20;
//
//        final var uploadPartRequests = new ArrayList<UploadPartRequest>();
//
//        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
//                .thenReturn(newInitiateMultipartUploadResult());
//        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class))).thenAnswer(a -> {
//            final var uploadPartRequest = (UploadPartRequest) a.getArgument(0);
//            // emulate behave of S3 client otherwise we will get wrong arrya in the memory
//            uploadPartRequest
//                    .setInputStream(new ByteArrayInputStream(uploadPartRequest.getInputStream().readAllBytes()));
//            uploadPartRequests.add(uploadPartRequest);
//
//            return newUploadPartResult(uploadPartRequest.getPartNumber(),
//                    "SOME_TAG#" + uploadPartRequest.getPartNumber());
//        });
//        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
//                .thenReturn(new CompleteMultipartUploadResult());
//
//        final var message = new byte[messageSize];
//
//        final var expectedFullMessage = new byte[messageSize + 10];
//        final var expectedTailMessage = new byte[10];
//
//        try (var outputStream = new S3OutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedAmazonS3)) {
//            random.nextBytes(message);
//            outputStream.write(message);
//            System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
//            random.nextBytes(message);
//            outputStream.write(message);
//            System.arraycopy(message, 0, expectedFullMessage, 20, 10);
//            System.arraycopy(message, 10, expectedTailMessage, 0, 10);
//        }
//
//        assertUploadPartRequest(uploadPartRequests.get(0), 30, 1, expectedFullMessage);
//        assertUploadPartRequest(uploadPartRequests.get(1), 10, 2, expectedTailMessage);
//
//        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
//        verify(mockedAmazonS3, times(2)).uploadPart(any(UploadPartRequest.class));
//        verify(mockedAmazonS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
//    }
//
//    private InitiateMultipartUploadResult newInitiateMultipartUploadResult() {
//        final var initiateMultipartUploadResult = new InitiateMultipartUploadResult();
//        initiateMultipartUploadResult.setUploadId(UPLOAD_ID);
//        return initiateMultipartUploadResult;
//    }
//
//    private UploadPartResult newUploadPartResult(final int partNumber, final String etag) {
//        final var uploadPartResult = new UploadPartResult();
//        uploadPartResult.setPartNumber(partNumber);
//        uploadPartResult.setETag(etag);
//        return uploadPartResult;
//    }
//
//    private void assertUploadPartRequest(final UploadPartRequest uploadPartRequest, final int expectedPartSize,
//            final int expectedPartNumber, final byte[] expectedBytes) throws IOException {
//        assertThat(uploadPartRequest.getPartSize()).isEqualTo(expectedPartSize);
//        assertThat(uploadPartRequest.getUploadId()).isEqualTo(UPLOAD_ID);
//        assertThat(uploadPartRequest.getPartNumber()).isEqualTo(expectedPartNumber);
//        assertThat(uploadPartRequest.getBucketName()).isEqualTo(BUCKET_NAME);
//        assertThat(uploadPartRequest.getKey()).isEqualTo(FILE_KEY);
//        assertThat(uploadPartRequest.getInputStream().readAllBytes()).isEqualTo(expectedBytes);
//    }

    private void assertCompleteMultipartUploadRequest(
            final CompleteMultipartUploadRequest completeMultipartUploadRequest) { //}, final List<PartETag> expectedETags) {
        assertThat(completeMultipartUploadRequest.bucket()).isEqualTo(BUCKET_NAME);
        assertThat(completeMultipartUploadRequest.key()).isEqualTo(FILE_KEY);
        assertThat(completeMultipartUploadRequest.uploadId()).isEqualTo(UPLOAD_ID);
    }

//    private void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest abortMultipartUploadRequest) {
//        assertThat(abortMultipartUploadRequest.getBucketName()).isEqualTo(BUCKET_NAME);
//        assertThat(abortMultipartUploadRequest.getKey()).isEqualTo(FILE_KEY);
//        assertThat(abortMultipartUploadRequest.getUploadId()).isEqualTo(UPLOAD_ID);
//    }

}
