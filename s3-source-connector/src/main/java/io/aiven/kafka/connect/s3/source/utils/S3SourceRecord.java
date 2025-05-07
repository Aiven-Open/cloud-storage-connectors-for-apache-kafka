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

import io.aiven.kafka.connect.common.NativeInfo;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.storage.NativeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3SourceRecord extends AbstractSourceRecord<String, S3Object, S3OffsetManagerEntry, S3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceRecord.class);

    public S3SourceRecord(final S3Object s3Object) {
        super(LOGGER, new NativeInfo<>() {
            @Override
            public S3Object getNativeItem() {
                return s3Object;
            }

            @Override
            public String getNativeKey() {
                return s3Object.key();
            }

            @Override
            public long getNativeItemSize() {
                return s3Object.size();
            }
        });
    }

    private S3SourceRecord(final S3SourceRecord s3SourceRecord) {
        super(s3SourceRecord);
    }

    @Override
    public S3SourceRecord duplicate() {
        return new S3SourceRecord(this);
    }

}
