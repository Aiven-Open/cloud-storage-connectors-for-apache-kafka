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

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.azure.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;

import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.blob.BlobServiceAsyncClient;

public class AzureBlobSourceConfig extends SourceCommonConfig {

    private final AzureBlobConfigFragment azureBlobConfigFragment;

    public AzureBlobSourceConfig(final Map<?, ?> properties) {
        super(new AzureBlobSourceConfigDef(), properties);
        azureBlobConfigFragment = new AzureBlobConfigFragment(this);
        validate();
    }

    public static ConfigDef configDef() {

        final var configDef = new AzureBlobSourceConfigDef();

        SourceConfigFragment.update(configDef);
        FileNameFragment.update(configDef);
        TransformerFragment.update(configDef);
        OutputFormatFragment.update(configDef, OutputFieldType.VALUE);
        AzureBlobConfigFragment.update(configDef);
        return configDef;
    }
    private void validate() {
    }

    public int getAzureFetchPageSize() {
        return azureBlobConfigFragment.getAzureFetchPageSize();
    }

    public String getAzurePrefix() {
        return azureBlobConfigFragment.getAzurePrefix();
    }

    public BlobServiceAsyncClient getAzureServiceAsyncClient() {
        return azureBlobConfigFragment.getAzureServiceAsyncClient();
    }

    public String getAzureContainerName() {
        return azureBlobConfigFragment.getContainerName();
    }

    public String getConnectionString() {
        return azureBlobConfigFragment.getConnectionString();
    }

    public String getContainerName() {
        return azureBlobConfigFragment.getContainerName();
    }

    public int getAzureRetryBackoffMaxAttempts() {
        return azureBlobConfigFragment.getAzureRetryBackoffMaxAttempts();
    }

    public Duration getAzureRetryBackoffInitialDelay() {
        return azureBlobConfigFragment.getAzureRetryBackoffInitialDelay();
    }

    public Duration getAzureRetryBackoffMaxDelay() {
        return azureBlobConfigFragment.getAzureRetryBackoffMaxDelay();
    }

    public String getUserAgent() {
        return azureBlobConfigFragment.getUserAgent();
    }

    public RetryOptions getAzureRetryOptions() {
        return azureBlobConfigFragment.getAzureRetryOptions();
    }
}
