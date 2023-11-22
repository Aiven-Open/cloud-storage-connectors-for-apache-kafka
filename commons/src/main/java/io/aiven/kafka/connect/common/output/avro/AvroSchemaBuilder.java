/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.connect.common.output.avro;

import java.util.Collection;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.SinkSchemaBuilder;

import io.confluent.connect.avro.AvroData;

public final class AvroSchemaBuilder extends SinkSchemaBuilder {

    public AvroSchemaBuilder(final Collection<OutputField> fields,
                             final AvroData avroData, final boolean envelopeEnabled) {
        super(fields, avroData, envelopeEnabled);
    }

    public AvroSchemaBuilder(final Collection<OutputField> fields, final AvroData avroData) {
        super(fields, avroData);
    }

    @Override
    protected String getNamespace() {
        return "io.aiven.avro.output.schema";
    }
}
