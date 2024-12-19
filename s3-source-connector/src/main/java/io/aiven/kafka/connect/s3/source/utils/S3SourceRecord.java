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

import java.util.Optional;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * The S3SourceRecord creates an immutable copy of the offsetManagerEntry, the recordKey and the recordValue.
 */
public final class S3SourceRecord {

    /** The S3OffsetManagerEntry for this source record */
    private final S3OffsetManagerEntry offsetManagerEntry;

    private final Optional<SchemaAndValue> recordKey;
    private final SchemaAndValue recordValue;

    public S3SourceRecord(final S3OffsetManagerEntry offsetManagerEntry, final Optional<SchemaAndValue> keyData,
            final SchemaAndValue valueData) {
        this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
        this.recordKey = keyData;
        this.recordValue = valueData;
    }

    public Object key() {
        return recordKey.map(SchemaAndValue::value).orElse(null);
    }

    public SchemaAndValue value() {
        return new SchemaAndValue(recordValue.schema(), recordValue.value());
    }

    public S3OffsetManagerEntry getOffsetManagerEntry() {
        return offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties()); // return a defensive copy
    }

    public String getObjectKey() {
        return offsetManagerEntry.getKey();
    }

    public SourceRecord getSourceRecord() {
        return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                offsetManagerEntry.getProperties(), offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(),
                recordKey.map(SchemaAndValue::schema).orElse(null), key(), recordValue.schema(), recordValue.value());
    }
}
