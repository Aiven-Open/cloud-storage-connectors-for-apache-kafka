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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

/**
 * The Source record that contains the offset manager data as well.
 */
public class S3SourceRecord {
    /** The S3OffsetManagerEntry for this source record */
    private final S3OffsetManagerEntry offsetManagerEntry;
    /** The Kakka record Key */
    private final byte[] recordKey;
    /** The Kafka record value */
    private final byte[] recordValue;

    public S3SourceRecord(final S3OffsetManagerEntry offsetManagerEntry, final byte[] recordKey,
            final byte[] recordValue) {
        // make defensive copies.
        this.recordKey = recordKey.clone();
        this.recordValue = recordValue.clone();
        this.offsetManagerEntry = offsetManagerEntry.fromProperties(offsetManagerEntry.getProperties());
    }

    public SourceRecord getSourceRecord(final Optional<Converter> keyConverter, final Converter valueConverter) {
        final Optional<SchemaAndValue> keyData = keyConverter
                .map(c -> c.toConnectData(offsetManagerEntry.getTopic(), recordKey));
        final SchemaAndValue schemaAndValue = valueConverter.toConnectData(offsetManagerEntry.getTopic(), recordValue);
        return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(),
                offsetManagerEntry.getProperties(), offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(),
                keyData.map(SchemaAndValue::schema).orElse(null), keyData.map(SchemaAndValue::value).orElse(null),
                schemaAndValue.schema(), schemaAndValue.value());
    }

    // package private for testing
    byte[] getRecordKey() {
        return recordKey.clone();
    }

    // package private for testing.
    byte[] getRecordValue() {
        return recordValue.clone();
    }
}
