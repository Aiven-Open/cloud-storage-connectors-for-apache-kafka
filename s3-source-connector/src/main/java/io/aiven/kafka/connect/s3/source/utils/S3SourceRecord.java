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
import org.apache.kafka.connect.source.SourceRecord;

public class S3SourceRecord {

    private final SchemaAndValue keyData;
    private final SchemaAndValue valueData;
    /** The S3OffsetManagerEntry for this source record */
    private final S3OffsetManagerEntry offsetManagerEntry;

    public S3SourceRecord(final S3OffsetManagerEntry offsetManagerEntry, final SchemaAndValue keyData,
            final SchemaAndValue valueData) {
        this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
        this.keyData = keyData;
        this.valueData = valueData;
    }

    public String getObjectKey() {
        return offsetManagerEntry.getKey();
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

    public SourceRecord getSourceRecord(final S3OffsetManagerEntry offsetManager) {
        return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                offsetManagerEntry.getProperties(), offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(),
                keyData.schema(), keyData.value(), valueData.schema(), valueData.value());
    }
}
