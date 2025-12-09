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

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import com.azure.storage.blob.BlobServiceAsyncClient;

public class AzureBlobSourceConfig extends SourceCommonConfig {

    // TODO AzureBlobFragment needs to be extracted from Azure Sink.
    private final AzureBlobConfigFragment azureBlobConfigFragment;
    public AzureBlobSourceConfig(final Map<String, String> properties) {
        super(new AzureBlobSourceConfigDef(), properties);
        azureBlobConfigFragment = new AzureBlobConfigFragment(dataAccess);
    }

    public int getAzureFetchPageSize() {
        return azureBlobConfigFragment.getAzureFetchPageSize();
    }

    public String getAzurePrefix() {
        return azureBlobConfigFragment.getAzurePrefix();
    }

    // TODO deprecate azure prefix and the azure prefix config option.
    @Override
    public String getPrefix() {
        return getAzurePrefix();
    }

    public BlobServiceAsyncClient getAzureServiceAsyncClient() {
        return azureBlobConfigFragment.getAzureServiceAsyncClient();
    }

    public String getAzureContainerName() {
        return azureBlobConfigFragment.getContainerName();
    }

    public int getFetchBufferSize() {
        return azureBlobConfigFragment.getFetchBufferSize();
    }

}
