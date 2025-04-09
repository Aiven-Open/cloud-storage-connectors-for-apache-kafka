/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.azure.source.utils;

import com.azure.core.http.rest.PagedFlux;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides unit tests to ensure basic functionality is maintained and works as expected.
 *
 * The AzureBlobClientIntegrationTest in the integration-test classes provides tests which ensure the api and client act as expected.
 */
class AzureBlobClientTest {

    public static final String TEST_CONTAINER = "test-container";
    private AzureBlobClient client;

    private AzureBlobSourceConfig config;

    private BlobContainerAsyncClient containerClient;
    private BlobAsyncClient blobClient;
    @BeforeEach
    public void setup() {
        this.config = mock(AzureBlobSourceConfig.class);
        final BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        this.blobClient = mock(BlobAsyncClient.class);
        this.containerClient = mock(BlobContainerAsyncClient.class);
        when(config.getAzurePrefix()).thenReturn(null);
        when(config.getAzureContainerName()).thenReturn(TEST_CONTAINER);
        when(config.getAzureServiceAsyncClient()).thenReturn(serviceClient);
        when(config.getAzureFetchPageSize()).thenReturn(1000);
        when(serviceClient.getBlobContainerAsyncClient(eq(TEST_CONTAINER))).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient(anyString())).thenReturn(blobClient);
    }

    @Test
    void testListAllBlobsReturnsAllBlobs() {
        client = new AzureBlobClient(config);
        final PagedFlux<BlobItem> flux = mock(PagedFlux.class);
        when(flux.toStream()).thenReturn(createListOfBlobs(100));
        when(containerClient.listBlobs(any())).thenReturn(flux);

        final Stream<BlobItem> stream = client.getAzureBlobStream();
        assertThat(stream).isNotNull();
        assertThat(stream.collect(Collectors.toList())).hasSize(100);
    }

    @Test
    void testListAllBlobsReturnsNullWhenNoEntriesAvailable() {
        client = new AzureBlobClient(config);
        when(containerClient.listBlobs(any())).thenReturn(new PagedFlux<>(Mono::empty, continuationToken -> null));
        final Stream<BlobItem> stream = client.getAzureBlobStream();
        assertThat(stream).isNotNull();
        assertThat(stream).isNullOrEmpty();
    }

    @Test
    void testGetBlobReceivesCanReturnNullWhenObjectIsEmpty() throws IOException {
        client = new AzureBlobClient(config);
        when(blobClient.downloadStream()).thenReturn(Flux.empty());
        try (InputStream inputStream = client.getBlob("teste-1")) {
            assertThat(inputStream.read()).isEqualTo(-1);
        }
    }

    @Test
    void testGetBlobReturnsDataAsExpected() throws IOException {
        client = new AzureBlobClient(config);
        final String blobContent = "This data is amazing";
        when(blobClient.downloadStream()).thenReturn(Flux.just(ByteBuffer.wrap(blobContent.getBytes(UTF_8))));
        try (InputStream inputStream = client.getBlob("teste-1");
            InputStreamReader reader = new InputStreamReader(inputStream, UTF_8);
            BufferedReader bufferedReader = new BufferedReader(reader)) {
            String s = bufferedReader.readLine();
            assertThat(s).isEqualTo(blobContent);
        }
    }

    private static Stream<BlobItem> createListOfBlobs(final int numberOfItems) {
        final List<BlobItem> items = new ArrayList<>();
        final BlobItemProperties props = new BlobItemProperties().setContentLength(10_000L);
        for (int i = 0; i < numberOfItems; i++) {
            final BlobItem item = new BlobItem();// NOPMD avoid creating new instances in a loop
            item.setName(String.valueOf(i));
            item.setProperties(props);
            items.add(item);
        }
        return items.stream();
    }

}
