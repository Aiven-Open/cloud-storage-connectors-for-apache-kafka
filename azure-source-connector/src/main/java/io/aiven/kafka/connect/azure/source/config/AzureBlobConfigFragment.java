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

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.aiven.kafka.connect.azure.source.utils.VersionInfo;
import io.aiven.kafka.connect.common.config.AbstractFragmentSetter;
import io.aiven.kafka.connect.common.config.ConfigFragment;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.Duration;
import java.util.Map;
/**
 * The configuration fragment that defines the Azure specific characteristics.
 */
public class AzureBlobConfigFragment extends ConfigFragment {

    public static final String AZURE_PREFIX_CONFIG = "azure.blob.prefix";
    public static final String AZURE_FETCH_PAGE_SIZE = "azure.blob.fetch.page.size";
    private static final String USER_AGENT_HEADER_FORMAT = "Azure Blob Source/%s (GPN: Aiven;)";
    public static final String USER_AGENT_HEADER_VALUE = String.format(USER_AGENT_HEADER_FORMAT,
            new VersionInfo().getVersion());
    private static final String GROUP_AZURE = "Azure";
    public static final String AZURE_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    public static final String AZURE_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";
    public static final String AZURE_USER_AGENT = "azure.user.agent";

    private static final String GROUP_AZURE_RETRY_BACKOFF_POLICY = "Azure retry backoff policy";

    public static final String AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "azure.retry.backoff.initial.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "azure.retry.backoff.max.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "azure.retry.backoff.max.attempts";

    public static final long AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;
    public static final long AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;
    public static final int AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    /**
     * Construct the Azure Blob ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    protected AzureBlobConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds the configuration options for the azure client to the configuration definition.
     *
     * @param configDef
     *            the Configuration definition.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef) {
        addUserAgentConfig(configDef);
        addAzureConfigGroup(configDef);
        addAzureRetryPolicies(configDef);
        return configDef;
    }

    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    private static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(AZURE_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting Azure");
    }

    private static void addAzureConfigGroup(final ConfigDef configDef) {
        int azureGroupCounter = 0;
        configDef.define(AZURE_STORAGE_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Azure Storage connection string.", GROUP_AZURE, azureGroupCounter++, ConfigDef.Width.NONE,
                AZURE_STORAGE_CONNECTION_STRING_CONFIG);

        configDef.define(AZURE_STORAGE_CONTAINER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                "The Azure Blob container name to store output files in.", GROUP_AZURE, azureGroupCounter++,
                ConfigDef.Width.NONE, AZURE_STORAGE_CONTAINER_NAME_CONFIG);
        configDef.define(AZURE_FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Azure Blob Fetch page size", GROUP_AZURE, azureGroupCounter++,
                ConfigDef.Width.NONE, AZURE_FETCH_PAGE_SIZE);
        configDef.define(AZURE_PREFIX_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "Prefix for stored objects, e.g. cluster-1/", GROUP_AZURE,
                azureGroupCounter++, ConfigDef.Width.NONE, AZURE_PREFIX_CONFIG); // NOPMD increment value never used
    }

    private static void addAzureRetryPolicies(final ConfigDef configDef) {
        int retryPolicyGroupCounter = 0;
        configDef.define(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Initial retry delay in milliseconds. The default value is "
                        + AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Maximum retry delay in milliseconds. The default value is " + AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, ConfigDef.Type.INT,
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Retry max attempts. The default value is " + AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE, // NOPMD
                // retryPolicyGroupCounter
                // updated value
                // never
                // used
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    @Override
    public void validate() {
        if (getConnectionString() == null) {
            throw new ConfigException(
                    String.format("The configuration %s cannot be null.", AZURE_STORAGE_CONNECTION_STRING_CONFIG));
        }
    }
    public int getAzureFetchPageSize() {
        return cfg.getInt(AZURE_FETCH_PAGE_SIZE);
    }

    public String getAzurePrefix() {
        return cfg.getString(AZURE_PREFIX_CONFIG);
    }

    public String getConnectionString() {
        return cfg.getString(AZURE_STORAGE_CONNECTION_STRING_CONFIG);
    }

    public String getContainerName() {
        return cfg.getString(AZURE_STORAGE_CONTAINER_NAME_CONFIG);
    }

    public int getAzureRetryBackoffMaxAttempts() {
        return cfg.getInt(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public Duration getAzureRetryBackoffInitialDelay() {
        return Duration.ofMillis(cfg.getLong(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    public Duration getAzureRetryBackoffMaxDelay() {
        return Duration.ofMillis(cfg.getLong(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    public String getUserAgent() {
        return cfg.getString(AZURE_USER_AGENT);
    }

    public RetryOptions getAzureRetryOptions() {
        return new RetryOptions(new ExponentialBackoffOptions().setMaxRetries(getAzureRetryBackoffMaxAttempts())
                .setBaseDelay(Duration.ofMillis(getAzureRetryBackoffInitialDelay().toMillis()))
                .setMaxDelay(Duration.ofMillis(getAzureRetryBackoffMaxDelay().toMillis())));
    }

    /**
     * Creates an async Service Client which can be used to create async container and blob clients, which can list and
     * download blobs respectively.
     *
     * @return A configured instance of BlobServiceAsyncClient
     */
    public BlobServiceAsyncClient getAzureServiceAsyncClient() {
        return new BlobServiceClientBuilder().connectionString(getConnectionString())
                .httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS))
                .addPolicy(new UserAgentPolicy(getUserAgent()))
                .retryOptions(getAzureRetryOptions())
                .buildAsyncClient();
    }

    public static class Setter extends AbstractFragmentSetter<AzureBlobConfigFragment.Setter> {
        private Setter(Map<String, String> data) {
            super(data);
        }

        public Setter prefix(final String prefix) {
            return setValue(AZURE_PREFIX_CONFIG, prefix);
        }

        public Setter fetchPageSize(final int fetchPageSize) {
            return setValue(AZURE_FETCH_PAGE_SIZE, fetchPageSize);
        }

        public Setter connectionString(final String connectionString) {
            return setValue(AZURE_STORAGE_CONNECTION_STRING_CONFIG, connectionString);
        }

        public Setter containerName(final String containerName) {
            return setValue(AZURE_STORAGE_CONTAINER_NAME_CONFIG, containerName);
        }

        public Setter userAgent(final String userAgent) {
            return setValue(AZURE_USER_AGENT, userAgent);
        }

        public Setter retryBackoffMaxAttempts(final int retryBackoffMaxAttempts) {
            return setValue(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, retryBackoffMaxAttempts);
        }

        public Setter retryBackoffInitialDelay(final Duration retryBackoffInitialDelay) {
            return setValue(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, retryBackoffInitialDelay.toMillis());
        }

        public Setter retryBackoffMaxDelay(final Duration retryBackoffInitialDelay) {
            return setValue(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, retryBackoffInitialDelay.toMillis());
        }
    }
}
