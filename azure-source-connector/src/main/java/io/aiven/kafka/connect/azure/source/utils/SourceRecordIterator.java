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

import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;

import com.azure.storage.blob.models.BlobItem;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes Azure Blob files and creates Kafka source records. Supports different output formats (Avro,
 * JSON, Parquet).
 */
public final class SourceRecordIterator
        extends
            AbstractSourceRecordIterator<BlobItem, String, AzureOffsetManagerEntry, AzureBlobSourceRecord> {

    /** The configuration for this Azure blob source */
    private final AzureBlobSourceConfig azureBlobSourceConfig;

    /** The azure blob client that provides the blobItems */
    private final AzureBlobClient azureBlobClient;

    /** The Azure container we are processing */
    private final String container;

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public SourceRecordIterator(final AzureBlobSourceConfig azureBlobSourceConfig,
            final OffsetManager<AzureOffsetManagerEntry> offsetManager, final Transformer transformer,
            final AzureBlobClient azureBlobClient) {
        super(azureBlobSourceConfig, offsetManager, transformer, 0);
        this.azureBlobSourceConfig = azureBlobSourceConfig;
        this.azureBlobClient = azureBlobClient;
        this.container = azureBlobSourceConfig.getAzureContainerName();
    }

    @Override
    protected Stream<AzureBlobSourceRecord> getSourceRecordStream(final String offset) {
        return azureBlobClient.getAzureBlobStream().map(fileMatching).filter(taskAssignment).map(Optional::get);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    protected IOSupplier<InputStream> getInputStream(final AzureBlobSourceRecord sourceRecord) {
        return () -> new ByteBufferInputStream(azureBlobClient.getBlob(sourceRecord.getNativeKey()).blockFirst());
    }

    @Override
    protected FilePatternUtils getFilePatternUtils(final SourceCommonConfig commonConfig) {
        AzureBlobSourceConfig azureBlobSourceConfig = (AzureBlobSourceConfig) commonConfig;
        return new FilePatternUtils(azureBlobSourceConfig.getAzureBlobFileNameFragment().getFilenameTemplate().toString());
    }

    @Override
    protected String getName(BlobItem nativeObject) {
        return nativeObject.getName();
    }

    @Override
    protected AzureBlobSourceRecord createSourceRecord(final BlobItem nativeObject) {
        return new AzureBlobSourceRecord(nativeObject);
    }

    @Override
    protected AzureOffsetManagerEntry createOffsetManagerEntry(final BlobItem nativeObject) {
        return new AzureOffsetManagerEntry(container, getName(nativeObject));
    }

    @Override
    protected OffsetManager.OffsetManagerKey getOffsetManagerKey() {
        return AzureOffsetManagerEntry.asKey(container, StringUtils.defaultIfBlank(getLastSeenNativeKey(), ""));
    }
}
