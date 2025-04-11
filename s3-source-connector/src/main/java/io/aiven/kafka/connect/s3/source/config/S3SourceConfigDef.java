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

package io.aiven.kafka.connect.s3.source.config;

import static io.aiven.kafka.connect.config.s3.S3CommonConfig.handleDeprecatedYyyyUppercase;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

public final class S3SourceConfigDef extends ConfigDef {

    public S3SourceConfigDef() {
        super();
        S3ConfigFragment.update(this);
        SourceConfigFragment.update(this);
        TransformerFragment.update(this);
        OutputFormatFragment.update(this, OutputFieldType.VALUE);
    }

    @Override
    public List<ConfigValue> validate(final Map<String, String> props) {
        return super.validate(handleDeprecatedYyyyUppercase(props));
    }
}
