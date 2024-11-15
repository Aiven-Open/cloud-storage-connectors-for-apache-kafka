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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

/**
 * The Source record that contains the offset manager data as well.
 */
public class S3SourceRecord {
    private final S3OffsetManagerEntry offsetManagerEntry;
    private final byte[] recordKey;
    private final byte[] recordValue;


    public S3SourceRecord(final S3OffsetManagerEntry offsetManagerEntry, final byte[] recordKey, final byte[] recordValue) {
        this.recordKey = recordKey.clone(); // Defensive copy
        this.recordValue = recordValue.clone(); // Defensive copy
        this.offsetManagerEntry = offsetManagerEntry;
    }


    public Map<String, Object> getPartitionMap() {
        return offsetManagerEntry.getManagerKey().getPartitionMap();
    }

    public Map<String, Object> getOffsetMap() {
        return offsetManagerEntry.getProperties();
    }

    public SourceRecord getSourceRecord(final Optional<Converter> keyConverter, final Converter valueConverter) {
        final Optional<SchemaAndValue> keyData = keyConverter.map(c -> c.toConnectData(offsetManagerEntry.getTopic(), recordKey));

        //transformer.configureValueConverter(conversionConfig, s3SourceConfig);
        //valueConverter.configure(conversionConfig, false);

        final SchemaAndValue schemaAndValue = valueConverter.toConnectData(offsetManagerEntry.getTopic(), recordValue);
//        offsetManager.updateCurrentOffsets(s3SourceRecord.getPartitionMap(), s3SourceRecord.getOffsetMap());
 //       s3SourceRecord.setOffsetMap(offsetManager.getOffsets().get(s3SourceRecord.getPartitionMap()));
//        return s3SourceRecord.getSourceRecord(keyData, schemaAndValue);

        return new SourceRecord(offsetManagerEntry.getManagerKey().getPartitionMap(), offsetManagerEntry.getProperties(), offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(),
                keyData.map(SchemaAndValue::schema).orElse(null), keyData.map(SchemaAndValue::value).orElse(null),
                schemaAndValue.schema(), schemaAndValue.value());
    }
}
