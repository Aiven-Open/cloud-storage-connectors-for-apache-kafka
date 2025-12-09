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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

public final class AzureBlobSinkConfig extends SinkCommonConfig {
    public static final String AZURE_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    public static final String AZURE_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";

    public static final long AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;
    public static final long AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;
    public static final int AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    private final AzureBlobConfigFragment azureFragment;

    public AzureBlobSinkConfig(final Map<String, String> properties) {
        super(new AzureBlobSinkConfigDef(), properties);
        azureFragment = new AzureBlobConfigFragment(dataAccess);
    }

    public String getConnectionString() {
        return azureFragment.getConnectionString();
    }

    public String getContainerName() {
        return azureFragment.getContainerName();
    }

    @Override
    public List<OutputField> getOutputFields() {
        return outputFormatFragment.getOutputFieldTypes()
                .stream()
                .map(fieldType -> fieldType == OutputFieldType.VALUE
                        ? new OutputField(fieldType, outputFormatFragment.getOutputFieldEncodingType())
                        : new OutputField(fieldType, OutputFieldEncodingType.NONE))
                .collect(Collectors.toList());
    }

    public int getAzureRetryBackoffMaxAttempts() {
        return azureFragment.getAzureRetryBackoffMaxAttempts();
    }

    public Duration getAzureRetryBackoffInitialDelay() {
        return azureFragment.getAzureRetryBackoffInitialDelay();
    }

    public Duration getAzureRetryBackoffMaxDelay() {
        return azureFragment.getAzureRetryBackoffMaxDelay();
    }

    public String getUserAgent() {
        return azureFragment.getUserAgent();
    }
}
