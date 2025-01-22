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

package io.aiven.kafka.connect.s3.source.utils;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3SourceRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceRecord.class);
    private SchemaAndValue keyData;
    private SchemaAndValue valueData;
    /** The S3OffsetManagerEntry for this source record */
    private S3OffsetManagerEntry offsetManagerEntry;
    private Context<String> context;
    private final S3Object s3Object;

    public S3SourceRecord(final S3Object s3Object) {
        this.s3Object = s3Object;
    }

    public S3SourceRecord(final S3SourceRecord s3SourceRecord) {
        this(s3SourceRecord.s3Object);
        this.offsetManagerEntry = s3SourceRecord.offsetManagerEntry
                .fromProperties(s3SourceRecord.getOffsetManagerEntry().getProperties());
        this.keyData = s3SourceRecord.keyData;
        this.valueData = s3SourceRecord.valueData;
        this.context = s3SourceRecord.context;
    }

    public void setOffsetManagerEntry(final S3OffsetManagerEntry offsetManagerEntry) {
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
        return context.getTopic().isPresent() ? context.getTopic().get() : null;
    }

    public Integer getPartition() {
        return context.getPartition().isPresent() ? context.getPartition().get() : null;
    }

    public String getObjectKey() {
        return s3Object.key();
    }

    public SchemaAndValue getKey() {
        return new SchemaAndValue(keyData.schema(), keyData.value());
    }

    public SchemaAndValue getValue() {
        return new SchemaAndValue(valueData.schema(), valueData.value());
    }

    public S3OffsetManagerEntry getOffsetManagerEntry() {
        return offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties()); // return a defensive copy
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
     * @return A kafka {@link org.apache.kafka.connect.source.SourceRecord SourceRecord}
     */
    public SourceRecord getSourceRecord(final ErrorsTolerance tolerance) {
        try {
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
