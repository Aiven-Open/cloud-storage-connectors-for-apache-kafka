/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.s3.config;

import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

public class S3SinkConfigDef extends SinkCommonConfig.SinkCommonConfigDef {

    public S3SinkConfigDef() {
        super(null, CompressionType.GZIP);
        S3ConfigFragment.update(this, true);
    }

    @Override
    public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
        final Map<String, ConfigValue> values = super.multiValidate(valueMap);
        final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
        new S3ConfigFragment(fragmentDataAccess).validate(values);
        return values;
    }
}
