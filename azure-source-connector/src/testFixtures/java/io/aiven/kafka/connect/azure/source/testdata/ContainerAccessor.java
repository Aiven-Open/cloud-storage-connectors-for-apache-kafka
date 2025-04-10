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

package io.aiven.kafka.connect.azure.source.testdata;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.aiven.kafka.connect.common.source.NativeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utility to access an Azure container
 */
public class ContainerAccessor {
    /** the name of the container to access */
    private final String containerName;
    /** the S3Client to access the container */
    private final BlobContainerClient containerClient;

    /** the logger to use */
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerAccessor.class);

    /**
     * Constructor.
     *
     * @param blobServiceClient
     *            the blob service client to use
     * @param containerName
     *            the container name to access.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable s3Client object")
    public ContainerAccessor(final BlobServiceClient blobServiceClient, final String containerName) {
        this.containerName = containerName;
        this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
        LOGGER.info("Created container client: {}", containerClient.getServiceVersion());
    }

    /**
     * Gets the container name this accessor is fronting.
     *
     * @return the container name.
     */
    public String getContainerName() {
        return containerName;
    }

    /**
     * Create the container.
     */
    public final void createContainer() {
        containerClient.createIfNotExists();
    }

    public final BlobClient getBlobClient(String blobName) {
        return containerClient.getBlobClient(blobName);
    }

    /**
     * Deletes the container.
     */
    public final void removeContainer() {
        containerClient.deleteIfExists();
    }

    public List<AzureNativeInfo> getNativeStorage() {
        return StreamSupport.stream(containerClient.listBlobs().spliterator(), false)
                .map(AzureNativeInfo::new)
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Implementation of NativeInfo for the azure blob access.
     */
    public static class AzureNativeInfo implements NativeInfo<BlobItem, String>, Comparable<AzureNativeInfo> {
        private BlobItem blobItem;

        AzureNativeInfo(BlobItem BlobItem) {
            this.blobItem = BlobItem;
        }

        @Override
        public BlobItem getNativeItem() {
            return blobItem;
        }

        @Override
        public String getNativeKey() {
            return blobItem.getName();
        }

        @Override
        public long getNativeItemSize() {
            return blobItem.getProperties().getContentLength();
        }

        @Override
        public int compareTo(AzureNativeInfo o) {
            return getNativeKey().compareTo(o.getNativeKey());
        }
    }
}
