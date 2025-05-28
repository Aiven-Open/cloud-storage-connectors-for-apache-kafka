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

import java.nio.ByteBuffer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import reactor.core.publisher.Flux;

/**
 * AzureBlobClient is a class that handles communication with the Azure blob source system/ It handles authentication,
 * querying and downloading of data from Azure in an Async manner.
 */
public class AzureBlobClient {

    private final AzureBlobSourceConfig config;
    private final BlobContainerAsyncClient containerAsyncClient;
    private final Predicate<BlobItem> filterPredicate = blobItem -> blobItem.getProperties().getContentLength() > 0;

    /**
     *
     * @param config
     *            AzureBlobSourceConfig with the configuration of how to connect to Azure and what Container to query
     *            for Blobs.
     */
    public AzureBlobClient(final AzureBlobSourceConfig config) {
        this.config = config;
        this.containerAsyncClient = config.getAzureServiceAsyncClient()
                .getBlobContainerAsyncClient(config.getAzureContainerName());
    }

    /**
     * returns a stream of BlobItems listing each object in lexical order. It handles paging of data within the client.
     *
     * @param offset
     *            the offset to start the list from.
     * @return A Stream of BlobItems in the container.
     *
     */
    public Stream<BlobItem> getAzureBlobStream(final String offset) {
        final ListBlobsOptions options = new ListBlobsOptions().setPrefix(config.getAzurePrefix())
                .setMaxResultsPerPage(config.getAzureFetchPageSize());
        return offset == null
                ? containerAsyncClient.listBlobs(options).toStream().filter(filterPredicate)
                : containerAsyncClient.listBlobs(options, offset).toStream().filter(filterPredicate);
    }

    /**
     *
     * @param blobName
     *            Name of the blob which is to be downloaded from Azure.
     * @return A Flux ByteArray, this Flux is an asynchronous implementation which returns 0..N parts
     */
    public Flux<ByteBuffer> getBlob(final String blobName) {
        return getBlobAsyncClient(blobName).downloadStream();
    }

    /**
     * Creates an Async BlobClient for a specific Blob in a container.
     *
     * @param blobName
     *            The name of the blob which the client is required for.
     * @return configured instance of BlobAsyncClient
     */
    private BlobAsyncClient getBlobAsyncClient(final String blobName) {
        return containerAsyncClient.getBlobAsyncClient(blobName);
    }
}
