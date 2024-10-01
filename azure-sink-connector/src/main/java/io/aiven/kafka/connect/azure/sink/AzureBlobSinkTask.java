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

package io.aiven.kafka.connect.azure.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AzureBlobSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSinkConnector.class);

    private RecordGrouper recordGrouper;
    private AzureBlobSinkConfig config;
    private BlobContainerClient containerClient;
    private final Map<String, BlockBlobClient> blobClientMap = new ConcurrentHashMap<>();

    // required by Connect
    public AzureBlobSinkTask() {
        super();
    }

    // for testing
    public AzureBlobSinkTask(final Map<String, String> props, final BlobServiceClient blobServiceClient) {
        super();
        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(blobServiceClient, "blobServiceClient cannot be null");

        this.config = new AzureBlobSinkConfig(props);
        this.containerClient = blobServiceClient.getBlobContainerClient(config.getContainerName());
        initRecordGrouper();
    }

    private BlockBlobClient getBlockBlobClient(final String blobName) {
        return blobClientMap.computeIfAbsent(blobName, name -> {
            final BlobClient blobClient = containerClient.getBlobClient(name);
            return blobClient.getBlockBlobClient();
        });
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.config = new AzureBlobSinkConfig(props);
        BlobServiceClient blobServiceClient;
        final UserAgentPolicy userAgentPolicy = new UserAgentPolicy(config.getUserAgent());

        final RetryOptions retryOptions = new RetryOptions(
                new ExponentialBackoffOptions().setMaxRetries(config.getAzureRetryBackoffMaxAttempts())
                        .setBaseDelay(Duration.ofMillis(config.getAzureRetryBackoffInitialDelay().toMillis()))
                        .setMaxDelay(Duration.ofMillis(config.getAzureRetryBackoffMaxDelay().toMillis())));

        blobServiceClient = new BlobServiceClientBuilder().connectionString(config.getConnectionString())
                .httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS))
                .addPolicy(userAgentPolicy)
                .retryOptions(retryOptions)
                .buildClient();
        this.containerClient = blobServiceClient.getBlobContainerClient(config.getContainerName());
        initRecordGrouper();

        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
    }

    private void initRecordGrouper() {
        try {
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        LOG.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            recordGrouper.records().forEach(this::flushFile);
        } finally {
            recordGrouper.clear();
        }
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        if (records.isEmpty()) {
            return;
        }

        final String blobName = config.getPrefix() + filename;
        final BlockBlobClient blockBlobClient = getBlockBlobClient(blobName);

        try (var channel = new BlobWritableByteChannel(blockBlobClient.getBlobOutputStream(true));
                OutputStream out = Channels.newOutputStream(channel);
                var outputWriter = OutputWriter.builder()
                        .withCompressionType(config.getCompressionType())
                        .withExternalProperties(config.originalsStrings())
                        .withOutputFields(config.getOutputFields())
                        .withEnvelopeEnabled(config.envelopeEnabled())
                        .build(out, config.getFormatType())) {

            LOG.debug("Opened BlobOutputStream for blob {}", blobName);

            outputWriter.writeRecords(records);
            LOG.debug("Successfully wrote records to blob {}", blobName);
        } catch (IOException e) {
            LOG.error("IOException when writing to the blob {}: {}", blobName, e.getMessage());
            throw new ConnectException(e);
        } catch (Exception e) { // NOPMD broad exception catched
            LOG.error("Exception when writing to the blob {}: {}", blobName, e.getMessage());
            throw new ConnectException("Failed to write records to Azure Blob", e);
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

}
