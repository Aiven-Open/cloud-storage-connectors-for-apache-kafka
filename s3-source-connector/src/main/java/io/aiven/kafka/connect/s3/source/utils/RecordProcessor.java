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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RecordProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessor.class);

    private RecordProcessor() {

    }

    public static List<SourceRecord> processRecords(final Iterator<AivenS3SourceRecord> sourceRecordIterator,
            final List<SourceRecord> results, final S3SourceConfig s3SourceConfig,
            final Optional<Converter> keyConverter, final Converter valueConverter,
            final AtomicBoolean connectorStopped, final Transformer transformer, final FileReader fileReader,
            final OffsetManager offsetManager) {

        final Map<String, String> conversionConfig = new HashMap<>();
        final int maxPollRecords = s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS);

        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final AivenS3SourceRecord aivenS3SourceRecord = sourceRecordIterator.next();
            if (aivenS3SourceRecord != null) {
                final SourceRecord sourceRecord = createSourceRecord(aivenS3SourceRecord, s3SourceConfig, keyConverter,
                        valueConverter, conversionConfig, transformer, fileReader, offsetManager);
                results.add(sourceRecord);
            }
        }

        return results;
    }

    static SourceRecord createSourceRecord(final AivenS3SourceRecord aivenS3SourceRecord,
            final S3SourceConfig s3SourceConfig, final Optional<Converter> keyConverter, final Converter valueConverter,
            final Map<String, String> conversionConfig, final Transformer transformer, final FileReader fileReader,
            final OffsetManager offsetManager) {

        final String topic = aivenS3SourceRecord.getTopic();
        final Optional<SchemaAndValue> keyData = keyConverter
                .map(c -> c.toConnectData(topic, aivenS3SourceRecord.key()));

        transformer.configureValueConverter(conversionConfig, s3SourceConfig);
        valueConverter.configure(conversionConfig, false);
        try {
            final SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, aivenS3SourceRecord.value());
            offsetManager.updateCurrentOffsets(aivenS3SourceRecord.getPartitionMap(),
                    aivenS3SourceRecord.getOffsetMap());
            aivenS3SourceRecord.setOffsetMap(offsetManager.getOffsets().get(aivenS3SourceRecord.getPartitionMap()));
            return aivenS3SourceRecord.getSourceRecord(topic, keyData, schemaAndValue);
        } catch (DataException e) {
            LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
            fileReader.addFailedObjectKeys(aivenS3SourceRecord.getObjectKey());
            throw e;
        }
    }
}
