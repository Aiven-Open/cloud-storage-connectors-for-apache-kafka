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

package io.aiven.kafka.connect.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ObjectWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWriter.class);

    private final OutputStream stream;
    private final OutputWriter outputWriter;
    private final String fullKey;
    private final String bucketName;
    @SuppressFBWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "OutputWriter can throw should handle in calling method")
    public S3ObjectWriter(final S3Client client, final S3SinkConfig config, final String fullKey) throws IOException {
        this.fullKey = fullKey;
        this.bucketName = config.getAwsS3BucketName();
        this.stream = buildS3OutputStream(fullKey, config.getAwsS3PartSize(),
                config.getServerSideEncryptionAlgorithmName(), client);
        this.outputWriter = OutputWriter.builder()
                .withCompressionType(config.getCompressionType())
                .withExternalProperties(config.originalsStrings())
                .withOutputFields(config.getOutputFields())
                .withEnvelopeEnabled(config.envelopeEnabled())
                .build(stream, config.getFormatType());
    }

    public void writeRecords(final List<SinkRecord> records) throws IOException {
        LOG.debug("Writing {} records to S3://{}/{}", records.size(), bucketName, fullKey);
        outputWriter.writeRecords(records);
    }

    private OutputStream buildS3OutputStream(final String fullKey, final int s3PartSize,
            final String serverSideEncryptionAlgorithmName, final S3Client client) {
        return new S3OutputStream(bucketName, fullKey, s3PartSize, client, serverSideEncryptionAlgorithmName);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing OutputWriter and OutputStream for s3://{}/{}", bucketName, fullKey);

        try (OutputStream ignored1 = stream; OutputWriter ignored = outputWriter) {
            // As per https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
            // if we were to call stream.close() and outputWriter.close()
            // if stream.close() were to throw an exception outputwriter would leak
            // using try-resource ensures close() is calle don both resources even if one throws.
        }
    }
}
