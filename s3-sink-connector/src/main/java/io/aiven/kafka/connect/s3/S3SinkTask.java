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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.s3.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("PMD.ExcessiveImports")
public final class S3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkTask.class);

    private RecordGrouper recordGrouper;

    private S3SinkConfig config;

    private S3Client s3Client;

    private Map<String, OutputWriter> writers;
    // stick in the config fragment
    private boolean isKeyRecordGrouper;

    S3ClientFactory s3ClientFactory = new S3ClientFactory();

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SinkTask() {
        super();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SinkConfig(props);
        s3Client = s3ClientFactory.createAmazonS3Client(config);
        writers = new HashMap<>();
        isKeyRecordGrouper = isOfTypeKeyRecordGrouper(config.getFilenameTemplate());
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
        if (!isKeyRecordGrouper) {
            recordGrouper.records().forEach(this::writeToS3);
        }
    }

    /**
     * This determines if the file is key based, and possible to change a single file multiple times per flush or if
     * it's a roll over file which at each flush is reset.
     *
     * @param fileNameTemplate
     *            the format type to output files in supplied in the configuration
     * @return true if is of type RecordGrouperFactory.KEY_RECORD or RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD
     */
    private boolean isOfTypeKeyRecordGrouper(final Template fileNameTemplate) {
        return RecordGrouperFactory.KEY_RECORD.equals(RecordGrouperFactory.resolveRecordGrouperType(fileNameTemplate))
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD
                        .equals(RecordGrouperFactory.resolveRecordGrouperType(fileNameTemplate));
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            recordGrouper.records().forEach(this::flushToS3);
        } finally {
            recordGrouper.clear();
        }
    }

    /**
     * getOutputWriter is used to check if an existing compatible OutputWriter exists and if not create one and return
     * it to the caller.
     *
     * @param filename
     *            used to write to S3
     * @param sinkRecord
     *            a sinkRecord used to create a new S3OutputStream
     * @return correct OutputWriter for writing a particular record to S3
     */
    private OutputWriter getOutputWriter(final String filename, final SinkRecord sinkRecord) {

        if (writers.get(filename) == null) {
            final var out = newStreamFor(filename, sinkRecord);
            try {
                writers.put(filename,
                        OutputWriter.builder()
                                .withCompressionType(config.getCompressionType())
                                .withExternalProperties(config.originalsStrings())
                                .withOutputFields(config.getOutputFields())
                                .withEnvelopeEnabled(config.envelopeEnabled())
                                .build(out, config.getFormatType()));
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
        return writers.get(filename);
    }

    /**
     *
     * @param filename
     *            the name of the file in S3 to be written to
     * @param records
     *            all records in this record grouping, including those already written to S3
     */
    private void writeToS3(final String filename, final List<SinkRecord> records) {
        // If no new records are supplied in this put operation return immediately
        Objects.requireNonNull(records, "records cannot be null");
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord sinkRecord = records.get(0);
        // Record Grouper returns all records for that filename, all we want is the new batch of records to be added
        // to the multi part upload.
        try {
            // This writer is being left open until a flush occurs.
            getOutputWriter(filename, sinkRecord).writeRecords(records);
            recordGrouper.clearProcessedRecords(filename, records);
        } catch (IOException e) {
            LOGGER.warn("Unable to write record, will retry on next put or flush operation.", e);
        }

    }

    /**
     * For Key record grouper the file is written just once to reduce the number of calls to S3 to a minimum. Each file
     * contains one record and is written once with the latest record when flush is called
     *
     * @param filename
     *            the name of the file in S3 to be written to
     * @param records
     *            all records in this record grouping, including those already written to S3
     */
    private void flushToS3(final String filename, final List<SinkRecord> records) {

        final SinkRecord sinkRecord = records.isEmpty() ? null : records.get(0);
        try (var writer = getOutputWriter(filename, sinkRecord)) {
            // For Key based files Record Grouper returns only one record for that filename
            // to the multi part upload.
            writer.writeRecords(records);
            writers.remove(filename, writer);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        if (!isKeyRecordGrouper) {
            writers.forEach((k, v) -> {
                try {
                    v.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            });
        }
        s3Client.close();
        LOGGER.info("Stop S3 Sink Task");
    }

    private String getFileNameTemplate(final String filename, final SinkRecord record) {
        return config.usesFileNameTemplate() ? filename : oldFullKey(record);
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    private OutputStream newStreamFor(final String filename, final SinkRecord record) {
        final var fullKey = getFileNameTemplate(filename, record);
        return new S3OutputStream(config.getAwsS3BucketName(), fullKey, config.getAwsS3PartSize(), s3Client,
                config.getServerSideEncryptionAlgorithmName());
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
