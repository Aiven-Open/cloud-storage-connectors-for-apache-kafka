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

package io.aiven.kafka.connect.s3.source;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AVRO_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.JSON_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public final class RecordProcessor {

    private RecordProcessor() {

    }
    public static List<SourceRecord> processRecords(final Iterator<S3SourceRecord> sourceRecordIterator,
            final List<SourceRecord> results, final S3SourceConfig s3SourceConfig,
            final Optional<Converter> keyConverter, final Converter valueConverter,
            final AtomicBoolean connectorStopped) {

        final Map<String, String> conversionConfig = new HashMap<>();
        final int maxPollRecords = s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS);

        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final S3SourceRecord record = sourceRecordIterator.next();
            final SourceRecord sourceRecord = createSourceRecord(record, s3SourceConfig, keyConverter, valueConverter,
                    conversionConfig);
            results.add(sourceRecord);
        }

        return results;
    }

    private static SourceRecord createSourceRecord(final S3SourceRecord record, final S3SourceConfig s3SourceConfig,
            final Optional<Converter> keyConverter, final Converter valueConverter,
            final Map<String, String> conversionConfig) {

        final String topic = record.getToTopic();
        final Optional<SchemaAndValue> keyData = keyConverter.map(c -> c.toConnectData(topic, record.key()));

        configureValueConverter(s3SourceConfig.getString(S3SourceConfig.OUTPUT_FORMAT), conversionConfig,
                s3SourceConfig);
        valueConverter.configure(conversionConfig, false);
        final SchemaAndValue value = valueConverter.toConnectData(topic, record.value());

        return new SourceRecord(record.getPartitionMap(), record.getOffsetMap(), topic, record.partition(),
                keyData.map(SchemaAndValue::schema).orElse(null), keyData.map(SchemaAndValue::value).orElse(null),
                value.schema(), value.value());
    }

    private static void configureValueConverter(final String outputFormat, final Map<String, String> config,
            final S3SourceConfig s3SourceConfig) {
        if (AVRO_OUTPUT_FORMAT.equals(outputFormat)) {
            config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
        } else if (JSON_OUTPUT_FORMAT.equals(outputFormat)) {
            config.put("schemas.enable", "false");
        }
    }
}
