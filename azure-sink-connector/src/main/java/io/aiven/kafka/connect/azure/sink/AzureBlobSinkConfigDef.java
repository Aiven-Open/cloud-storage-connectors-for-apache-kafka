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

package io.aiven.kafka.connect.azure.sink;

import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

public final class AzureBlobSinkConfigDef extends SinkCommonConfig.SinkCommonConfigDef {

    AzureBlobSinkConfigDef() {
        super(OutputFieldType.VALUE, CompressionType.NONE);
        AzureBlobConfigFragment.update(this, true);
    }

    @Override
    public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
        final Map<String, ConfigValue> result = super.multiValidate(valueMap);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(result);
        new AzureBlobConfigFragment(dataAccess).validate(result);
        return result;
    }
}
