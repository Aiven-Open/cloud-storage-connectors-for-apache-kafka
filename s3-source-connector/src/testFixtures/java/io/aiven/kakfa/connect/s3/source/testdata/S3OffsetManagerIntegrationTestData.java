/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kakfa.connect.s3.source.testdata;/*
                                                  * Copyright 2025 Aiven Oy
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

import java.util.Map;
import java.util.function.BiFunction;

import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;

/**
 * Configures data objects in a standard format for S3 tests.
 */
public final class S3OffsetManagerIntegrationTestData {

    private S3OffsetManagerIntegrationTestData() {
        // do not instantiate
    }

    /**
     * Creatse a ManagerEntryFactor that returns S3OffsetManagerEntries.
     *
     * @return a function to create an S3OffsetManagerEntry from the Kafka defined partitionMap and data.
     */
    public static BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData::getOffsetManagerEntry;
    }

    /**
     * Creates an S3OffsetManagerEntry from the Kafka defined partitionMap and data.
     *
     * @param key
     *            the Kafka defined partitionMap
     * @param data
     *            the Kafka data map.
     * @return the S3OffsetManagerEntry.
     */
    public static S3OffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key,
            final Map<String, Object> data) {
        final S3OffsetManagerEntry entry = new S3OffsetManagerEntry(key.get(S3OffsetManagerEntry.BUCKET).toString(),
                key.get(S3OffsetManagerEntry.OBJECT_KEY).toString());
        return entry.fromProperties(data);
    }
}
