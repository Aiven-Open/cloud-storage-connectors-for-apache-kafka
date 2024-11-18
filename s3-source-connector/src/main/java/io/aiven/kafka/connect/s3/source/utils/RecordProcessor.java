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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public final class RecordProcessor {

    // private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessor.class);

    private RecordProcessor() {

    }

    public static List<SourceRecord> processRecords(final Iterator<S3SourceRecord> sourceRecordIterator,
            final List<SourceRecord> results, final S3SourceConfig s3SourceConfig,
            final Optional<Converter> keyConverter, final Converter valueConverter,
            final AtomicBoolean connectorStopped) {

        final int maxPollRecords = s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS);

        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            results.add(s3SourceRecord.getSourceRecord(keyConverter, valueConverter));
        }

        return results;
    }
    //
    // static SourceRecord createSourceRecord(final S3SourceRecord s3SourceRecord, final S3SourceConfig s3SourceConfig,
    // final Optional<Converter> keyConverter, final Converter valueConverter,
    // final Map<String, String> conversionConfig, final Transformer transformer, final FileReader fileReader,
    // final OffsetManager offsetManager) {
    //
    // final String topic = s3SourceRecord.getTopic();
    // final Optional<SchemaAndValue> keyData = keyConverter.map(c -> c.toConnectData(topic, s3SourceRecord.key()));
    //
    // transformer.configureValueConverter(conversionConfig, s3SourceConfig);
    // valueConverter.configure(conversionConfig, false);
    // try {
    // final SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, s3SourceRecord.value());
    // offsetManager.updateCurrentOffsets(s3SourceRecord.getPartitionMap(), s3SourceRecord.getOffsetMap());
    // s3SourceRecord.setOffsetMap(offsetManager.getOffsets().get(s3SourceRecord.getPartitionMap()));
    // return s3SourceRecord.getSourceRecord(keyData, schemaAndValue);
    // } catch (DataException e) {
    // LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
    // fileReader.addFailedObjectKeys(s3SourceRecord.getObjectKey());
    // throw e;
    // }
    // }
}
