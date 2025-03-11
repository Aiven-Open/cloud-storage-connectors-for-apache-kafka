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

package io.aiven.kafka.connect.azure.source.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import com.azure.storage.blob.models.BlobItem;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes Azure Blob files and creates Kafka source records. Supports different output formats (Avro,
 * JSON, Parquet).
 */
public final class SourceRecordIterator implements Iterator<AzureBlobSourceRecord> {
    /** The OffsetManager that we are using */
    private final OffsetManager<AzureOffsetManagerEntry> offsetManager;

    /** The configuration for this Azure blob source */
    private final AzureBlobSourceConfig azureBlobSourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** The azure blob client that provides the blobItems */
    private final AzureBlobClient azureBlobClient;
    /** the taskId of this running task */
    private int taskId;

    /** The Azure container we are processing */
    private final String container;

    /**
     * The inner iterator to provides a base AzureBlobSourceRecord for an Blob that has passed the filters and
     * potentially had data extracted.
     */
    private Iterator<AzureBlobSourceRecord> inner;
    /**
     * The outer iterator that provides an AzureBlobSourceRecord for each record contained by the Blob identified by the
     * inner record.
     */
    private Iterator<AzureBlobSourceRecord> outer;
    /** The topic(s) which have been configured with the 'topics' configuration */
    private final Optional<String> targetTopics;

    /** Check if the blob name is part of the 'target' files configured to be extracted from Azure */
    final FileMatching fileMatching;
    /** The predicate which will determine if an Azure Blob should be assigned to this task for processing */
    final Predicate<Optional<AzureBlobSourceRecord>> taskAssignment;
    /** The utility to extract the context from the Blob name */
    private FilePatternUtils filePattern;
    /**
     * The blob which is currently being processed, when rehydrating from Azure we will skip other blob items until
     * after this item
     */
    private String lastSeenBlobName;

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public SourceRecordIterator(final AzureBlobSourceConfig azureBlobSourceConfig,
            final OffsetManager<AzureOffsetManagerEntry> offsetManager, final Transformer transformer,
            final AzureBlobClient azureBlobClient) {

        super();
        this.azureBlobSourceConfig = azureBlobSourceConfig;
        this.offsetManager = offsetManager;
        this.container = azureBlobSourceConfig.getAzureContainerName();
        this.transformer = transformer;
        this.azureBlobClient = azureBlobClient;
        this.targetTopics = Optional.ofNullable(azureBlobSourceConfig.getTargetTopic());
        this.taskAssignment = new TaskAssignment(initializeDistributionStrategy());
        this.taskId = azureBlobSourceConfig.getTaskId();
        this.fileMatching = new FileMatching(filePattern);

        inner = getAzureBlobSourceRecordStream(azureBlobClient).iterator();
        outer = Collections.emptyIterator();
    }

    private Stream<AzureBlobSourceRecord> getAzureBlobSourceRecordStream(final AzureBlobClient client) {
        return client.getAzureBlobStream().map(fileMatching).filter(taskAssignment).map(Optional::get);
    }

    @Override
    public boolean hasNext() {
        if (!outer.hasNext()) {
            // Remove the last seen Blob from the offsets as the file has been completely processed.
            offsetManager.removeEntry(
                    AzureOffsetManagerEntry.asKey(container, StringUtils.defaultIfBlank(lastSeenBlobName, "")));
        }
        if (!inner.hasNext() && !outer.hasNext()) {
            inner = getAzureBlobSourceRecordStream(azureBlobClient).iterator();
        }
        while (!outer.hasNext() && inner.hasNext()) {
            outer = convert(inner.next()).iterator();
        }
        return outer.hasNext();
    }

    @Override
    public AzureBlobSourceRecord next() {
        return outer.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * Converts the Blob Item into stream of AzureBlobSourceRecords.
     *
     * @param azureBlobSourceRecord
     *            the SourceRecord that drives the creation of source records with values.
     * @return a stream of AzureBlobSourceRecords created from the input stream of the Blob.
     */
    private Stream<AzureBlobSourceRecord> convert(final AzureBlobSourceRecord azureBlobSourceRecord) {
        azureBlobSourceRecord.setKeyData(transformer.getKeyData(azureBlobSourceRecord.getBlobName(),
                azureBlobSourceRecord.getTopic(), azureBlobSourceConfig));

        lastSeenBlobName = azureBlobSourceRecord.getBlobName();

        return transformer
                .getRecords(
                        () -> new ByteBufferInputStream(
                                azureBlobClient.getBlob(azureBlobSourceRecord.getBlobName()).blockFirst()),
                        azureBlobSourceRecord.getAzureBlobSize(), azureBlobSourceRecord.getContext(),
                        azureBlobSourceConfig, azureBlobSourceRecord.getRecordCount())
                .map(new Mapper(azureBlobSourceRecord));

    }

    private DistributionStrategy initializeDistributionStrategy() {
        final DistributionType distributionType = azureBlobSourceConfig.getDistributionType();
        final int maxTasks = azureBlobSourceConfig.getMaxTasks();
        this.taskId = azureBlobSourceConfig.getTaskId() % maxTasks;
        this.filePattern = new FilePatternUtils(
                azureBlobSourceConfig.getAzureBlobFileNameFragment().getFilenameTemplate().toString());
        return distributionType.getDistributionStrategy(maxTasks);
    }

    /**
     * maps the data from the @{link Transformer} stream to an AzureBlobSourceRecord given all the additional data
     * required.
     */
    static class Mapper implements Function<SchemaAndValue, AzureBlobSourceRecord> {
        /**
         * The AzureBlobSourceRecord that produceces the values.
         */
        private final AzureBlobSourceRecord sourceRecord;

        public Mapper(final AzureBlobSourceRecord sourceRecord) {
            // operation within the Transformer
            // to see if there are more records.
            this.sourceRecord = sourceRecord;
        }

        @Override
        public AzureBlobSourceRecord apply(final SchemaAndValue valueData) {
            sourceRecord.incrementRecordCount();
            final AzureBlobSourceRecord result = new AzureBlobSourceRecord(sourceRecord);
            result.setValueData(valueData);
            return result;
        }
    }

    class TaskAssignment implements Predicate<Optional<AzureBlobSourceRecord>> {
        final DistributionStrategy distributionStrategy;

        TaskAssignment(final DistributionStrategy distributionStrategy) {
            this.distributionStrategy = distributionStrategy;
        }

        @Override
        public boolean test(final Optional<AzureBlobSourceRecord> azureBlobSourceRecord) {
            if (azureBlobSourceRecord.isPresent()) {
                final AzureBlobSourceRecord record = azureBlobSourceRecord.get();
                final Context<String> context = record.getContext();
                return taskId == distributionStrategy.getTaskFor(context);

            }
            return false;
        }

    }

    class FileMatching implements Function<BlobItem, Optional<AzureBlobSourceRecord>> {

        final FilePatternUtils utils;
        FileMatching(final FilePatternUtils utils) {
            this.utils = utils;
        }

        @Override
        public Optional<AzureBlobSourceRecord> apply(final BlobItem blobItem) {

            final Optional<Context<String>> optionalContext = utils.process(blobItem.getName());
            if (optionalContext.isPresent()) {
                final AzureBlobSourceRecord azureBlobSourceRecord = new AzureBlobSourceRecord(blobItem);
                final Context<String> context = optionalContext.get();
                overrideContextTopic(context);
                azureBlobSourceRecord.setContext(context);
                AzureOffsetManagerEntry offsetManagerEntry = new AzureOffsetManagerEntry(container, blobItem.getName());
                offsetManagerEntry = offsetManager
                        .getEntry(offsetManagerEntry.getManagerKey(), offsetManagerEntry::fromProperties)
                        .orElse(offsetManagerEntry);
                azureBlobSourceRecord.setOffsetManagerEntry(offsetManagerEntry);
                return Optional.of(azureBlobSourceRecord);
            }
            return Optional.empty();
        }

        private void overrideContextTopic(final Context<String> context) {
            // Set the target topic in the context if it has been set from configuration.
            if (targetTopics.isPresent()) {
                if (context.getTopic().isPresent()) {
                    LOGGER.debug(
                            "Overriding topic '{}' extracted from Blob name with topic '{}' from configuration 'topics'. ",
                            context.getTopic().get(), targetTopics.get());
                }
                context.setTopic(targetTopics.get());
            }
        }
    }

}
