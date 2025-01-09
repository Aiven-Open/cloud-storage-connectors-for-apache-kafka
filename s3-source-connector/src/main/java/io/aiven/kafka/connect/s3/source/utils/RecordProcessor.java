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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RecordProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessor.class);

    private RecordProcessor() {
    }

    public static SourceRecord createSourceRecord(final S3SourceRecord s3SourceRecord,
            final S3SourceConfig s3SourceConfig, final AWSV2SourceClient sourceClient,
            final S3OffsetManagerEntry s3OffsetManagerEntry) {
        try {
            return s3SourceRecord.getSourceRecord(s3OffsetManagerEntry);
        } catch (DataException e) {
            if (ErrorsTolerance.NONE.equals(s3SourceConfig.getErrorsTolerance())) {
                throw new ConnectException("Data Exception caught during S3 record to source record transformation", e);
            } else {
                sourceClient.addFailedObjectKeys(s3SourceRecord.getObjectKey());
                LOGGER.warn(
                        "Data Exception caught during S3 record to source record transformation {} . errors.tolerance set to 'all', logging warning and continuing to process.",
                        e.getMessage(), e);
                return null;
            }
        }
    }
}
