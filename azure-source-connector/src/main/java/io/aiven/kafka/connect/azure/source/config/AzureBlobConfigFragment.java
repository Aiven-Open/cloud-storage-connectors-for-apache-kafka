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

import static java.lang.String.format;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.AbstractFragmentSetter;
import io.aiven.kafka.connect.common.config.ConfigFragment;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.common.utils.VersionInfo;

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

/**
 * The configuration fragment that defines the Azure specific characteristics.
 */
public final class AzureBlobConfigFragment extends ConfigFragment {
    /**
     * Valid protocols for Azure blob access
     */
    public enum Protocol {
        HTTP, HTTPS
    }
    private static final VersionInfo VERSION_INFO = new VersionInfo(AzureBlobConfigFragment.class);
    private static final String AZURE_PREFIX_CONFIG = "azure.blob.prefix";
    private static final String AZURE_FETCH_PAGE_SIZE = "azure.blob.fetch.page.size";
    private static final String USER_AGENT_HEADER_FORMAT = "Azure Blob Source/%s (Vendor: %s)";
    private static final String USER_AGENT_HEADER_VALUE = format(USER_AGENT_HEADER_FORMAT, VERSION_INFO.getVersion(),
            VERSION_INFO.getVendor());
    private static final String GROUP_AZURE = "Azure";
    public static final String AZURE_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    public static final String AZURE_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";
    public static final String AZURE_USER_AGENT = "azure.user.agent";

    private static final String GROUP_AZURE_RETRY_BACKOFF_POLICY = "Azure retry backoff policy";
    public static final String AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "azure.retry.backoff.initial.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "azure.retry.backoff.max.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "azure.retry.backoff.max.attempts";

    public static final long AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = Duration.ofSeconds(1).toMillis();
    public static final long AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = Duration.ofSeconds(32).toMillis();
    public static final int AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    /**
     * "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;"
     */
    private static final String AZURE_ACCOUNT_NAME_CONFIG = "azure.account.name";
    private static final String AZURE_ACCOUNT_KEY_CONFIG = "azure.account.key";
    private static final String AZURE_BLOB_ENDPOINT_CONFIG = "azure.blob.endpoint";
    private static final String AZURE_ENDPOINT_PROTOCOL_CONFIG = "azure.endpoint_protocol";

    /**
     * Construct the Azure Blob ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public AzureBlobConfigFragment(final AbstractConfig cfg) {
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

    /**
     * Gets the setter for this fragment.
     *
     * @param data
     *            the data map to update.
     * @return the setter for this fragment.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    private static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(AZURE_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting Azure");
    }

    private static void addAzureConfigGroup(final ConfigDef configDef) {
        int azureGroupCounter = 0;

        configDef.define(AZURE_ACCOUNT_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Azure Account id", GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.MEDIUM,
                AZURE_ACCOUNT_NAME_CONFIG);

        configDef.define(AZURE_ACCOUNT_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Azure Account key", GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.LONG, AZURE_ACCOUNT_KEY_CONFIG);

        configDef.define(AZURE_BLOB_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null, new UrlValidator(),
                ConfigDef.Importance.HIGH, "Azure Blob source endpoint", GROUP_AZURE, ++azureGroupCounter,
                ConfigDef.Width.LONG, AZURE_BLOB_ENDPOINT_CONFIG);

        configDef.define(AZURE_ENDPOINT_PROTOCOL_CONFIG, ConfigDef.Type.STRING, Protocol.HTTPS.name(),
                new ProtocolValidator(), ConfigDef.Importance.HIGH, "Azure endpoint protocol (http or https)",
                GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.SHORT, AZURE_ENDPOINT_PROTOCOL_CONFIG);

        configDef.define(AZURE_STORAGE_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Azure Storage connection string.", GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.NONE,
                AZURE_STORAGE_CONNECTION_STRING_CONFIG);

        configDef.define(AZURE_STORAGE_CONTAINER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                "The Azure Blob container name to store output files in.", GROUP_AZURE, ++azureGroupCounter,
                ConfigDef.Width.NONE, AZURE_STORAGE_CONTAINER_NAME_CONFIG);

        configDef.define(AZURE_FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Azure Blob Fetch page size", GROUP_AZURE, ++azureGroupCounter,
                ConfigDef.Width.NONE, AZURE_FETCH_PAGE_SIZE);

        configDef.define(AZURE_PREFIX_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "Prefix for stored objects, e.g. cluster-1/", GROUP_AZURE,
                ++azureGroupCounter, ConfigDef.Width.NONE, AZURE_PREFIX_CONFIG);
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
            if (has(AZURE_ACCOUNT_KEY_CONFIG) && has(AZURE_ACCOUNT_NAME_CONFIG) && has(AZURE_BLOB_ENDPOINT_CONFIG)
                    && has(AZURE_ENDPOINT_PROTOCOL_CONFIG)) {
                return;
            }
            throw new ConfigException(format("Either %s must be specified or %s, %s, &s, and %s must be specified.",
                    AZURE_STORAGE_CONNECTION_STRING_CONFIG, AZURE_ACCOUNT_KEY_CONFIG, AZURE_ACCOUNT_NAME_CONFIG,
                    AZURE_BLOB_ENDPOINT_CONFIG, AZURE_ENDPOINT_PROTOCOL_CONFIG));
        }
    }

    /** Creates a URL formatted string */
    private String createURL(final String url) {
        return url.contains("://") ? url : format("%s://%s", getEndpointProtocol(), url);
    }

    /**
     * Gets the azure fetch page size. This is the number of items to receive information about on a single call to the
     * backend.
     *
     * @return the fetch page size.
     */
    public int getAzureFetchPageSize() {
        return cfg.getInt(AZURE_FETCH_PAGE_SIZE);
    }

    /**
     * Gets the Azure prefix. This is a string that must appear at the beginning of every item returned.
     *
     * @return the Azure blob prefix.
     */
    public String getAzurePrefix() {
        return cfg.getString(AZURE_PREFIX_CONFIG);
    }

    /**
     * Get the connection string for the backend.
     *
     * @return the Azure connection string
     */
    public String getConnectionString() {
        if (has(AZURE_STORAGE_CONNECTION_STRING_CONFIG)) {
            return cfg.getString(AZURE_STORAGE_CONNECTION_STRING_CONFIG);
        }
        return format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
                getEndpointProtocol(), getAccountName(), getAccountKey(), getBlobEndpoint(), getContainerName());
    }

    /**
     * Gets the Azure account name.
     *
     * @return the Azure account name.
     */
    public String getAccountName() {
        return cfg.getString(AZURE_ACCOUNT_NAME_CONFIG);
    }

    /**
     * Gets the Azure account key.
     *
     * @return the Azure account key.
     */
    public String getAccountKey() {
        return cfg.getString(AZURE_ACCOUNT_KEY_CONFIG);
    }

    /**
     * Gets the blob endpoint URL as a string.
     *
     * @return the blob endpoint URL.
     */
    public String getBlobEndpoint() {
        return createURL(cfg.getString(AZURE_BLOB_ENDPOINT_CONFIG));
    }

    /**
     * Gets the default endpoint protocol.
     *
     * @return the endpoint protocol
     */
    public Protocol getEndpointProtocol() {
        return Protocol.valueOf(cfg.getString(AZURE_ENDPOINT_PROTOCOL_CONFIG));
    }

    /**
     * Gets the container name.
     *
     * @return the container name.
     */
    public String getContainerName() {
        return cfg.getString(AZURE_STORAGE_CONTAINER_NAME_CONFIG);
    }

    /**
     * Gets the retry backoff max attempts.
     *
     * @return The retry backoff max attempts.
     */
    public int getAzureRetryBackoffMaxAttempts() {
        return cfg.getInt(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    /**
     * Gets the retry backoff initial delay.
     *
     * @return The retry backoff initial delay.
     */
    public Duration getAzureRetryBackoffInitialDelay() {
        return Duration.ofMillis(cfg.getLong(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    /**
     * Gets the retry backoff max delay.
     *
     * @return The retry backoff max delay.
     */
    public Duration getAzureRetryBackoffMaxDelay() {
        return Duration.ofMillis(cfg.getLong(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    /**
     * Gets the user agent.
     *
     * @return The user agent.
     */
    public String getUserAgent() {
        return cfg.getString(AZURE_USER_AGENT);
    }

    /**
     * Gets the RetryOptions.
     *
     * @return a RetryOptions containing the retry values.
     */
    public RetryOptions getAzureRetryOptions() {
        return new RetryOptions(new ExponentialBackoffOptions().setMaxRetries(getAzureRetryBackoffMaxAttempts())
                .setBaseDelay(Duration.ofMillis(getAzureRetryBackoffInitialDelay().toMillis()))
                .setMaxDelay(Duration.ofMillis(getAzureRetryBackoffMaxDelay().toMillis())));
    }

    /**
     * Creates an async Service Client which talks to the specified server.
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

    @SuppressWarnings({ "PMD.TooManyMethods" })
    public static class Setter extends AbstractFragmentSetter<AzureBlobConfigFragment.Setter> {
        /**
         * Creates the setter
         *
         * @param data
         *            the data to modify.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the Azure prefix.
         *
         * @param prefix
         *            the Azuere prefix.
         * @return this
         */
        public Setter prefix(final String prefix) {
            return setValue(AZURE_PREFIX_CONFIG, prefix);
        }

        /**
         * Sets the fetch page size.
         *
         * @param fetchPageSize
         *            the fetch page size.
         * @return this
         */
        public Setter fetchPageSize(final int fetchPageSize) {
            return setValue(AZURE_FETCH_PAGE_SIZE, fetchPageSize);
        }

        /**
         * Sets the connection string.
         *
         * @param connectionString
         *            the connection string.
         * @return this.
         */
        public Setter connectionString(final String connectionString) {
            return setValue(AZURE_STORAGE_CONNECTION_STRING_CONFIG, connectionString);
        }

        /**
         * Sets the account name.
         *
         * @param name
         *            the account name.
         * @return this,
         */
        public Setter accountName(final String name) {
            return setValue(AZURE_ACCOUNT_NAME_CONFIG, name);
        }

        /**
         * Sets the account key.
         *
         * @param key
         *            the account key.
         * @return this.
         */
        public Setter accountKey(final String key) {
            return setValue(AZURE_ACCOUNT_KEY_CONFIG, key);
        }

        /**
         * Sets the blob endpoint.
         *
         * @param blobEndpoint
         *            the blob endpoint.
         * @return this
         */
        public Setter blobEndpoint(final String blobEndpoint) {
            return setValue(AZURE_BLOB_ENDPOINT_CONFIG, blobEndpoint);
        }

        /**
         * Sets the default endpoing protocol.
         *
         * @param protocol
         *            the endpoint protocol.
         * @return this.
         */
        public Setter endpointProtocol(final Protocol protocol) {
            return setValue(AZURE_ENDPOINT_PROTOCOL_CONFIG, protocol.toString());
        }

        /**
         * Sets the container name.
         *
         * @param containerName
         *            the container name.
         * @return this.
         */
        public Setter containerName(final String containerName) {
            return setValue(AZURE_STORAGE_CONTAINER_NAME_CONFIG, containerName);
        }

        /**
         * Sets the user agent.
         *
         * @param userAgent
         *            the user agent.
         * @return this.
         */
        public Setter userAgent(final String userAgent) {
            return setValue(AZURE_USER_AGENT, userAgent);
        }

        /**
         * Sets the retry backoff max attempts.
         *
         * @param retryBackoffMaxAttempts
         *            the retry backoff max attempts.
         * @return this
         */
        public Setter retryBackoffMaxAttempts(final int retryBackoffMaxAttempts) {
            return setValue(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, retryBackoffMaxAttempts);
        }

        /**
         * Sets the backoff initial delay.
         *
         * @param retryBackoffInitialDelay
         *            the backoff initial delay.
         * @return this
         */
        public Setter retryBackoffInitialDelay(final Duration retryBackoffInitialDelay) {
            return setValue(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, retryBackoffInitialDelay.toMillis());
        }

        /**
         * Sets the backoff maximum delay.
         *
         * @param retryBackoffMaximumDelay
         *            the retry backoff maximum delay.
         * @return this
         */
        public Setter retryBackoffMaxDelay(final Duration retryBackoffMaximumDelay) {
            return setValue(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, retryBackoffMaximumDelay.toMillis());
        }
    }

    /**
     * The protocol validator for the default protocol.
     */
    public static class ProtocolValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(final String name, final Object value) {
            // is it up to the connector decide how to support default values for compression.
            // The reason is that for different connectors there is the different compression type
            if (Objects.nonNull(value)) {
                try {
                    Protocol.valueOf(value.toString());
                } catch (final IllegalArgumentException e) {
                    throw new ConfigException(name, value.toString(), toString());
                }
            }
        }

        @Override
        public String toString() {
            return "Supported Values are: "
                    + Arrays.stream(Protocol.values()).map(Object::toString).collect(Collectors.joining(", ")) + ".";
        }
    }

}
