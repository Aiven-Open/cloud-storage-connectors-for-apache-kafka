/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.kafka.connect.azure.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.OutputWriter;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AzureBlobWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSinkTask.class);

    private final BlobWritableByteChannel channel;
    private final OutputWriter outputWriter;
    private final String fullPath;
    private final String containerName;

    @SuppressWarnings("PMD.CloseResource")
    public AzureBlobWriter(final BlobContainerClient containerClient, final AzureBlobSinkConfig config,
            final String filename) throws IOException {
        this.containerName = config.getContainerName();
        this.fullPath = config.getPrefix() + filename;
        final BlockBlobClient blockBlobClient = containerClient.getBlobClient(fullPath).getBlockBlobClient();
        // getBlobOutputStream(true) opens a block-staging stream: records are streamed to Azure as
        // they are written, so a whole file is never held in memory.
        this.channel = new BlobWritableByteChannel(blockBlobClient.getBlobOutputStream(true));
        final OutputStream out = Channels.newOutputStream(channel);
        this.outputWriter = OutputWriter.builder()
                .withExternalProperties(config.originalsStrings())
                .withOutputFields(config.getOutputFields())
                .withCompressionType(config.getCompressionType())
                .withEnvelopeEnabled(config.envelopeEnabled())
                .build(out, config.getFormatType()); // This line can throw IOException
    }

    public void writeRecords(final List<SinkRecord> records) throws IOException {
        LOG.debug("Writing {} records to container {} blob {}", records.size(), containerName, fullPath);
        outputWriter.writeRecords(records);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing OutputWriter and channel for container {} blob {}", containerName, fullPath);

        try (BlobWritableByteChannel ignored1 = channel; OutputWriter ignored = outputWriter) {
            // Explicitly empty
        }
    }
}
