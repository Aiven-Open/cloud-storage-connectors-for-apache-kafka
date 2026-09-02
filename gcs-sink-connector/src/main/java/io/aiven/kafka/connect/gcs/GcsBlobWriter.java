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

package io.aiven.kafka.connect.gcs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.OutputWriter;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsBlobWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkTask.class);

    private final WriteChannel channel;
    private final OutputWriter outputWriter;
    private final String fullPath;
    private final String bucketName;

    @SuppressWarnings("PMD.CloseResource")
    public GcsBlobWriter(final Storage storage, final GcsSinkConfig config, final String filename) throws IOException {
        this.bucketName = config.getBucketName();
        this.fullPath = config.getPrefix() + filename;
        final BlobInfo blob = BlobInfo.newBuilder(bucketName, fullPath)
                .setContentEncoding(config.getObjectContentEncoding())
                .build();
        this.channel = storage.writer(blob);
        final OutputStream out = Channels.newOutputStream(channel);
        this.outputWriter = OutputWriter.builder()
                .withExternalProperties(config.originalsStrings())
                .withOutputFields(config.getOutputFields())
                .withCompressionType(config.getCompressionType())
                .withEnvelopeEnabled(config.envelopeEnabled())
                .build(out, config.getFormatType()); // This line can throw IOException
    }

    public void writeRecords(final List<SinkRecord> records) throws IOException {
        LOG.debug("Writing {} records to gs://{}/{}", records.size(), bucketName, fullPath);
        outputWriter.writeRecords(records);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing OutputWriter and WriteChannel for gs://{}/{}", bucketName, fullPath);

        try (WriteChannel ignored1 = channel; OutputWriter ignored = outputWriter) {
            // Explicitly empty
        }
    }
}
