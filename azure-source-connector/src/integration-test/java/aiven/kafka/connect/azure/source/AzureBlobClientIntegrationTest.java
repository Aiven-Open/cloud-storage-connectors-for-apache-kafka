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

package aiven.kafka.connect.azure.source;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;

import com.azure.storage.blob.BlobServiceAsyncClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AzureBlobClientIntegrationTest extends AbstractIntegrationTest<byte[], byte[]> {
    private static final String CONNECTOR_NAME = "aiven-azure-source-connector";

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBlobAccessor.clear(azurePrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        super.startConnectRunner(producerProps);
    }

    @Test
    void azureBlobClientListsAllBlobs() {
        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
        when(config.getAzureContainerName()).thenReturn("test-1");
        final AzureBlobClient client = new AzureBlobClient(config);
        // Test the pagination is automatically executed.
        when(config.getAzureFetchPageSize()).thenReturn(10);
        when(config.getAzurePrefix()).thenReturn(azurePrefix);

        final String blobName = "blob-%d.txt";
        final String contents = "Azure Blob sanity integration and testing %d";
        for (int i = 0; i < 500; i++) {

            testBlobAccessor.createBlob(azurePrefix, String.format(blobName, i),
                    new ByteArrayInputStream(String.format(contents, i).getBytes(UTF_8)));// NOPMD
                                                                                          // avoid
                                                                                          // instantiating
                                                                                          // in loops
        }
        assertThat(testBlobAccessor.getBlobNames()).hasSize(500);

        assertThat(client.getAzureBlobStream(null).collect(Collectors.toList())).hasSize(500);
    }

    @Test
    void azureBlobClientGetBlobs() {
        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
        when(config.getAzureContainerName()).thenReturn("test-1");
        when(config.getAzurePrefix()).thenReturn(azurePrefix);
        final AzureBlobClient client = new AzureBlobClient(config);
        // Test the pagintation is automatically executed.
        when(config.getAzureFetchPageSize()).thenReturn(1);
        when(config.getAzurePrefix()).thenReturn(null);

        for (int i = 0; i < 10; i++) {
            // blob name and contents are the same.
            testBlobAccessor.createBlob(azurePrefix, "Blob" + i,
                    new ByteArrayInputStream(("Blob" + i).getBytes(UTF_8)));// NOPMD
            // avoid
            // instantiating
            // in
            // loops
        }
        assertThat(testBlobAccessor.getBlobNames()).hasSize(10);

        client.getAzureBlobStream(null).forEach(blobitem -> {

            assertThat(blobitem.getName()).isEqualTo(azurePrefix + new String(
                    Objects.requireNonNull(client.getBlob(blobitem.getName()).blockFirst()).array(), UTF_8));// NOPMD
            // avoid
            // instantiating
            // in
            // loops
        });

    }

    @Test
    void azureBlobClientListBlobsWhenContainerEmpty() {
        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
        when(config.getAzureContainerName()).thenReturn("test-1");
        final AzureBlobClient client = new AzureBlobClient(config);
        when(config.getAzureFetchPageSize()).thenReturn(1);
        when(config.getAzurePrefix()).thenReturn(null);

        assertThat(client.getAzureBlobStream(null)).isNullOrEmpty();

    }

}
