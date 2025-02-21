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

package io.aiven.kafka.connect.azure.source.config;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;

public class AzureBlobSourceConfig extends SourceCommonConfig {

    // TODO AzureBlobFragment needs to be extracted from Azure Sink.
    private final FileNameFragment azureBlobFileNameFragment;
    public AzureBlobSourceConfig(final Map<?, ?> properties) {
        super(new ConfigDef(), properties);
        azureBlobFileNameFragment = new FileNameFragment(this);
        validate();
    }

    public static ConfigDef configDef() {

        final var configDef = new AzureBlobSourceConfigDef();

        SourceConfigFragment.update(configDef);
        FileNameFragment.update(configDef);
        TransformerFragment.update(configDef);
        OutputFormatFragment.update(configDef, OutputFieldType.VALUE);

        return configDef;
    }
    private void validate() {
    }

    public FileNameFragment getAzureBlobFileNameFragment() {
        return azureBlobFileNameFragment;
    }
}
