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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
     * @return A Stream of BlobItems in the container.
     *
     */
    public Stream<BlobItem> getAzureBlobStream() {
        final ListBlobsOptions options = new ListBlobsOptions().setPrefix(config.getAzurePrefix())
                .setMaxResultsPerPage(config.getAzureFetchPageSize());
        return containerAsyncClient.listBlobs(options).toStream().filter(filterPredicate);
    }

    /**
     *
     * @param blobName
     *            Name of the blob which is to be downloaded from Azure.
     * @return A Flux ByteArray, this Flux is an asynchronous implementation which returns 0..N parts
     */
    public InputStream getBlob(final String blobName) {
        return new FluxToInputStream(getBlobAsyncClient(blobName).downloadStream());
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

    private static class FluxToInputStream extends InputStream {
        private static final int EOF = -1;
        private Iterator<ByteBuffer> iterator;
        private ByteBuffer current;

        FluxToInputStream(Flux<ByteBuffer> flux) {
            iterator = flux.toStream().iterator();
        }

        private void checkCurrent() {
            if (current == null) {
                if (iterator.hasNext()) {
                    current = iterator.next();
                } else {
                    current = ByteBuffer.allocate(0);
                }

            }
            if (current != null && !current.hasRemaining()) {
                if (iterator.hasNext()) {
                    current = iterator.next();
                }
            }
        }

        @Override
        public int read() throws IOException {
            checkCurrent();
            return current.hasRemaining() ? current.get() & 0xFF : EOF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            checkCurrent();
            if (!current.hasRemaining()) {
                return EOF;
            }
            int copyLen = Math.min(current.remaining(), len);
            current.get(b, off, copyLen);
            return copyLen;
        }

        @Override
        public void close() throws IOException {
            current = null;
            iterator = null;
        }
    }
}
