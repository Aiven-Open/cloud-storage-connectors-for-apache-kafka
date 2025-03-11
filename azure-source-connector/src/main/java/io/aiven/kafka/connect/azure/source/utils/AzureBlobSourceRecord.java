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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.task.Context;

import com.azure.storage.blob.models.BlobItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobSourceRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceRecord.class);
    private SchemaAndValue keyData;
    private SchemaAndValue valueData;
    /** The AzureOffsetManagerEntry for this source record */
    private AzureOffsetManagerEntry offsetManagerEntry;
    private Context<String> context;
    private final BlobItem blobItem;

    public AzureBlobSourceRecord(final BlobItem blobItem) {
        this.blobItem = blobItem;
    }

    public AzureBlobSourceRecord(final AzureBlobSourceRecord azureBlobSourceRecord) {
        this(azureBlobSourceRecord.blobItem);
        this.offsetManagerEntry = azureBlobSourceRecord.offsetManagerEntry
                .fromProperties(azureBlobSourceRecord.getOffsetManagerEntry().getProperties());
        this.keyData = azureBlobSourceRecord.keyData;
        this.valueData = azureBlobSourceRecord.valueData;
        this.context = azureBlobSourceRecord.context;
    }

    public void setOffsetManagerEntry(final AzureOffsetManagerEntry offsetManagerEntry) {
        this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
    }

    public long getRecordCount() {
        return offsetManagerEntry == null ? 0 : offsetManagerEntry.getRecordCount();
    }

    public void setKeyData(final SchemaAndValue keyData) {
        this.keyData = keyData;
    }

    public void incrementRecordCount() {
        this.offsetManagerEntry.incrementRecordCount();
    }

    public void setValueData(final SchemaAndValue valueData) {
        this.valueData = valueData;
    }

    public String getTopic() {
        return context.getTopic().orElse(null);
    }

    public Integer getPartition() {
        return context.getPartition().orElse(null);
    }

    public String getBlobName() {
        return blobItem.getName();
    }

    public SchemaAndValue getKey() {
        return new SchemaAndValue(keyData.schema(), keyData.value());
    }

    public SchemaAndValue getValue() {
        return new SchemaAndValue(valueData.schema(), valueData.value());
    }

    public AzureOffsetManagerEntry getOffsetManagerEntry() {
        return offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties()); // return a defensive copy
    }

    public long getAzureBlobSize() {
        return blobItem.getProperties().getContentLength();
    }

    public Context<String> getContext() {
        return new Context<>(context) {
        };

    }
    public void setContext(final Context<String> context) {
        this.context = new Context<>(context) {
        };
    }

    /**
     * Creates a SourceRecord that can be returned to a Kafka topic
     *
     * @return A kafka {@link SourceRecord SourceRecord} This can return null if error tolerance is set to 'All'
     */
    public SourceRecord getSourceRecord(final ErrorsTolerance tolerance,
            final OffsetManager<AzureOffsetManagerEntry> offsetManager) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Source Record: {} for Topic: {} , Partition: {}, recordCount: {}", getBlobName(),
                        getTopic(), getPartition(), getRecordCount());
            }
            offsetManager.addEntry(offsetManagerEntry);
            return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                    offsetManagerEntry.getProperties(), getTopic(), getPartition(), keyData.schema(), keyData.value(),
                    valueData.schema(), valueData.value());
        } catch (DataException e) {
            if (ErrorsTolerance.NONE.equals(tolerance)) {
                throw new ConnectException("Data Exception caught during S3 record to source record transformation", e);
            } else {
                LOGGER.warn(
                        "Data Exception caught during S3 record to source record transformation {} . errors.tolerance set to 'all', logging warning and continuing to process.",
                        e.getMessage(), e);
                return null;
            }
        }
    }

}
