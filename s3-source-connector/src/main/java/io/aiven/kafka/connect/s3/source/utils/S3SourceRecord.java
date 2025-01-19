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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3SourceRecord implements Cloneable {

    private SchemaAndValue keyData;
    private SchemaAndValue valueData;
    /** The S3OffsetManagerEntry for this source record */
    private S3OffsetManagerEntry offsetManagerEntry;

    private final S3Object s3Object;

    public S3SourceRecord(final S3Object s3Object) {
        this.s3Object = s3Object;
    }

    @Override
    public S3SourceRecord clone() throws CloneNotSupportedException {
        super.clone();
        final S3SourceRecord result = new S3SourceRecord(s3Object);
        result.setOffsetManagerEntry(offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties()));
        result.keyData = keyData;
        result.valueData = valueData;
        return result;
    }
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "offsetManagerEntry is essentially read only")
    public void setOffsetManagerEntry(final S3OffsetManagerEntry offsetManagerEntry) {
        this.offsetManagerEntry = offsetManagerEntry;
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
        return offsetManagerEntry.getTopic();
    }

    public int getPartition() {
        return offsetManagerEntry.getPartition();
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

    public SourceRecord getSourceRecord(final S3OffsetManagerEntry offsetManager) {
        return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                offsetManagerEntry.getProperties(), offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(),
                keyData.schema(), keyData.value(), valueData.schema(), valueData.value());
    }
}
