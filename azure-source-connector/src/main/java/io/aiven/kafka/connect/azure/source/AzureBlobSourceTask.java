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

package io.aiven.kafka.connect.azure.source;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecord;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceTask;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.utils.VersionInfo;

import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobSourceTask extends AbstractSourceTask {
    /* The logger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceTask.class);

    private OffsetManager<AzureBlobOffsetManagerEntry> offsetManager;
    /** The configuration for this run */
    private AzureBlobSourceConfig azureBlobSourceConfig; // NOPMD only called once, when used in the future this can be
    // removed

    /**
     * Constructor to set the Logger used. This constructor is required by Connect.
     *
     */
    public AzureBlobSourceTask() {
        super(LOGGER);
    }

    /**
     * For testing access.
     *
     * @param iterator
     *            the iterator to read.
     */
    protected AzureBlobSourceTask(final Iterator<AzureBlobSourceRecord> iterator) {
        this();
        azureBlobSourceRecordIterator = iterator;
    }

    @Override
    protected Iterator<SourceRecord> getIterator(final BackoffConfig config) {
        final Iterator<SourceRecord> inner = IteratorUtils.transformedIterator(azureBlobSourceRecordIterator,
                r -> r.getSourceRecord(azureBlobSourceConfig.getErrorsTolerance(), offsetManager));
        return IteratorUtils.filteredIterator(inner, Objects::nonNull);
    }

    @Override
    protected SourceCommonConfig configure(final Map<String, String> props) {
        this.azureBlobSourceConfig = new AzureBlobSourceConfig(props);
        offsetManager = new OffsetManager<>(context);
        final AzureBlobClient azureBlobClient = new AzureBlobClient(azureBlobSourceConfig);
        azureBlobSourceRecordIterator = new AzureBlobSourceRecordIterator(azureBlobSourceConfig, offsetManager,
                azureBlobSourceConfig.getTransformer(), azureBlobClient);
        return azureBlobSourceConfig;
    }

    @Override
    protected void closeResources() {
        // nothing to do.
    }

    @Override
    public String version() {
        return new VersionInfo().getVersion();
    }
}
