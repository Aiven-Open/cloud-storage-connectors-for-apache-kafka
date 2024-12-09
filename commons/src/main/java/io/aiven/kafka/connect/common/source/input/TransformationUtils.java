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

package io.aiven.kafka.connect.common.source.input;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;

final public class TransformationUtils {
    private static final AvroData AVRO_DATA = new AvroData(100);

    private TransformationUtils() {
        // hidden
    }

    public static SchemaAndValue getValueStruct(final Object record, final String topic,
            final AbstractConfig sourceConfig) {
        return AVRO_DATA.toConnectData(((GenericRecord) record).getSchema(), record);
    }
}
