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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RecordProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessor.class);

    private RecordProcessor() {

    }

    public static List<SourceRecord> processRecords(final Iterator<S3SourceRecord> sourceRecordIterator,
            final List<SourceRecord> results, final S3SourceConfig s3SourceConfig, final AtomicBoolean connectorStopped,
            final AWSV2SourceClient sourceClient, final OffsetManager offsetManager) {

        // final Map<String, String> conversionConfig = new HashMap<>();
        final int maxPollRecords = s3SourceConfig.getMaxPollRecords();

        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            if (s3SourceRecord != null) {
                final SourceRecord sourceRecord = createSourceRecord(s3SourceRecord, sourceClient, offsetManager);
                results.add(sourceRecord);
            }
        }

        return results;
    }

    static SourceRecord createSourceRecord(final S3SourceRecord s3SourceRecord,

            final AWSV2SourceClient sourceClient, final OffsetManager offsetManager) {
        final String topic = s3SourceRecord.getTopic();
        final SchemaAndValue keyData = s3SourceRecord.getKeyData();
        final SchemaAndValue valueData = s3SourceRecord.getValueData();
        try {
            offsetManager.updateCurrentOffsets(s3SourceRecord.getPartitionMap(), s3SourceRecord.getOffsetMap());
            s3SourceRecord.setOffsetMap(offsetManager.getOffsets().get(s3SourceRecord.getPartitionMap()));
            return s3SourceRecord.getSourceRecord(topic, keyData, valueData);
        } catch (DataException e) {
            LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
            sourceClient.addFailedObjectKeys(s3SourceRecord.getObjectKey());
            throw e;
        }
    }
}
