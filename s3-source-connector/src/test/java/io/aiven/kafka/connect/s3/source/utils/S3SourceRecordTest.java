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

import io.aiven.kafka.connect.common.source.AbstractSourceRecordTest;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3SourceRecordTest extends AbstractSourceRecordTest<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> {
    @Override
    protected String createKFrom(String key) {
        return key;
    }

    @Override
    protected S3OffsetManagerEntry createOffsetManagerEntry(String key) {
        return new S3OffsetManagerEntry("bucket1", key);
    }

    @Override
    protected S3SourceRecord createSourceRecord() {
        S3Object object = S3Object.builder().key("key").size(5L).build();
        return new S3SourceRecord(object);
    }
}
