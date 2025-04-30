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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;

import io.aiven.kafka.connect.azure.config.AzureBlobConfigFragment;

import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.blob.BlobServiceAsyncClient;
import org.junit.jupiter.api.Test;

final class AzureBlobConfigTest {
    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        /*
         * This is the string that is expected to be generated given the values below. This is the connect string for
         * Azure.
         */
        final String expectedConnectionString = "DefaultEndpointsProtocol=HTTPS;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;BlobEndpoint=HTTPS://example.com/TheContainer;";
        // azure props
        AzureBlobConfigFragment.setter(props)
                .blobEndpoint("example.com")
                .accountKey("ACCOUNT_KEY")
                .accountName("ACCOUNT_NAME")
                .containerName("TheContainer")
                .prefix("Prefix")
                .fetchPageSize(10)
                .retryBackoffInitialDelay(Duration.ofSeconds(1))
                .retryBackoffMaxAttempts(5)
                .retryBackoffMaxDelay(Duration.ofSeconds(30))
                .userAgent("myAgent");

        final AzureBlobSourceConfig conf = new AzureBlobSourceConfig(props);
        // azure blob
        assertThat(conf.getConnectionString()).isEqualTo(expectedConnectionString);
        assertThat(conf.getAzureContainerName()).isEqualTo("TheContainer");
        assertThat(conf.getAzurePrefix()).isEqualTo("Prefix");
        assertThat(conf.getAzureFetchPageSize()).isEqualTo(10);
        assertThat(conf.getAzureRetryBackoffInitialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(conf.getAzureRetryBackoffMaxAttempts()).isEqualTo(5);
        assertThat(conf.getAzureRetryBackoffMaxDelay()).isEqualTo(Duration.ofSeconds(30));
        final RetryOptions retryOptions = conf.getAzureRetryOptions();
        assertThat(retryOptions).isNotNull();
        final BlobServiceAsyncClient client = conf.getAzureServiceAsyncClient();
        assertThat(client).isNotNull();
    }
}
