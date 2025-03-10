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

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.aiven.kafka.connect.config.s3.S3ClientFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.StableTimeFormatter;
import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("PMD.ExcessiveImports")
public final class S3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private RecordGrouper recordGrouper;

    private S3SinkConfig config;

    private S3Client s3Client;

    AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SinkTask() {
        super();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SinkConfig(props);

        s3Client = new S3ClientFactory().createAmazonS3Client(config);

        try {
            recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD AvoidCatchingGenericException
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
    }


    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        LOGGER.info("Processing {} records", records.size());
        records.forEach(recordGrouper::put);
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
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
        final SinkRecord sinkRecord = records.get(0);
        try (var out = newStreamFor(filename, sinkRecord);
                var outputWriter = OutputWriter.builder()
                        .withCompressionType(config.getCompressionType())
                        .withExternalProperties(config.originalsStrings())
                        .withOutputFields(config.getOutputFields())
                        .withEnvelopeEnabled(config.envelopeEnabled())
                        .build(out, config.getFormatType())) {
            outputWriter.writeRecords(records);
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        //s3Client.shutdown();
        LOGGER.info("Stop S3 Sink Task");
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    private OutputStream newStreamFor(final String filename, final SinkRecord record) {
        final var fullKey = config.usesFileNameTemplate() ? filename : oldFullKey(record);
        return new S3OutputStream(config, fullKey, s3Client);
    }



    private String oldFullKey(final SinkRecord record) {
        final var prefix = config.getPrefixTemplate()
                .instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name,
                        new StableTimeFormatter(config.getTimestampSource()).apply(record))
                .bindVariable(FilenameTemplateVariable.PARTITION.name, () -> record.kafkaPartition().toString())
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name,
                        parameter -> OldFullKeyFormatters.KAFKA_OFFSET.apply(record, parameter))
                .bindVariable(FilenameTemplateVariable.TOPIC.name, record::topic)
                .bindVariable("utc_date",
                        () -> ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE))
                .bindVariable("local_date", () -> LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE))
                .render();
        final var key = String.format("%s-%s-%s", record.topic(), record.kafkaPartition(),
                OldFullKeyFormatters.KAFKA_OFFSET.apply(record, VariableTemplatePart.Parameter.of("padding", "true")));
        // Keep this in line with io.aiven.kafka.connect.common.config.AivenCommonConfig#getFilename
        final String formatSuffix = FormatType.AVRO.equals(config.getFormatType()) ? ".avro" : "";
        return prefix + key + formatSuffix + config.getCompressionType().extension();
    }

}
