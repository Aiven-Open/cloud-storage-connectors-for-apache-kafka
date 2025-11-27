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

package io.aiven.kafka.connect.azure.sink;

import java.time.Duration;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.ConfigFragment;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

/**
 * The configuration fragment that defines the Azure specific characteristics. TODO merge this with the Azure source
 * version.
 */
public final class AzureBlobConfigFragment extends ConfigFragment {

    public static final String AZURE_PREFIX_CONFIG = "azure.blob.prefix";
    public static final String AZURE_FETCH_PAGE_SIZE = "azure.blob.fetch.page.size";
    private static final String USER_AGENT_HEADER_FORMAT = "Azure Blob Source/%s (GPN: Aiven;)";
    public static final String USER_AGENT_HEADER_VALUE = String.format(USER_AGENT_HEADER_FORMAT, Version.VERSION);
    private static final String GROUP_AZURE = "Azure";
    public static final String AZURE_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    public static final String AZURE_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";
    public static final String AZURE_USER_AGENT = "azure.user.agent";

    private static final String GROUP_AZURE_RETRY_BACKOFF_POLICY = "Azure retry backoff policy";
    private static final String AZURE_FETCH_BUFFER_SIZE = "azure.blob.fetch.buffer.size";
    public static final String AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "azure.retry.backoff.initial.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "azure.retry.backoff.max.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "azure.retry.backoff.max.attempts";

    public static final long AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;
    public static final long AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;
    public static final int AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    private final static Pattern CONTAINER_NAME_PATTERN = Pattern.compile("[0-9a-z][0-9a-z\\-]+[0-9a-z]");

    /**
     * From Azure documentation:
     * <ul>
     * <li>Container names must start or end with a letter or number, and can contain only letters, numbers, and the
     * hyphen/minus (-) character.</li>
     * <li>Every hyphen/minus (-) character must be immediately preceded and followed by a letter or number; consecutive
     * hyphens aren't permitted in container names.</li>
     * <li>All letters in a container name must be lowercase.</li>
     * <li>Container names must be from 3 through 63 characters long.</li>
     * </ul>
     */
    private static final ConfigDef.Validator CONTAINER_NAME_VALIDATOR = ConfigDef.CompositeValidator
            .of(ConfigDef.LambdaValidator.with((name, value) -> {
                final int len = value == null ? 0 : value.toString().length();
                if (len < 3 || len > 63) {
                    throw new ConfigException(name, value, "names must be from 3 through 63 characters long.");
                }
            }, () -> "must be from 3 through 63 characters long"), ConfigDef.LambdaValidator.with((name, value) -> {
                if (value.toString().contains("--")) {
                    throw new ConfigException(name, value,
                            "Every hyphen/minus (-) character must be immediately preceded and followed by a letter or number; consecutive hyphens aren't permitted in container names.");
                }
            }, () -> "consecutive hyphens aren't permitted in container names"),
                    // regex last for speed
                    ConfigDef.LambdaValidator.with((name, value) -> {
                        if (!CONTAINER_NAME_PATTERN.matcher(value.toString()).matches()) {
                            throw new ConfigException(name, value,
                                    "must start or end with a letter or number, and can contain only lower case letters, numbers, and the hyphen/minus (-) character.");
                        }
                    }, () -> "start or end with a letter or number, and can contain only lower case letters, numbers, and the hyphen/minus (-) character"));

    /**
     * Construct the Azure Blob ConfigFragment..
     *
     * @param dataAccess
     *            the configuration that this fragment is associated with.
     */
    public AzureBlobConfigFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Adds the configuration options for the azure client to the configuration definition.
     *
     * @param configDef
     *            the Configuration definition.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef, final boolean isSink) {
        addUserAgentConfig(configDef);
        addAzureConfigGroup(configDef, isSink);
        addAzureRetryPolicies(configDef);
        return configDef;
    }

    private static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(AZURE_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting Azure");
    }

    private static void addAzureConfigGroup(final ConfigDef configDef, final boolean isSink) {
        int azureGroupCounter = 0;
        configDef.define(AZURE_STORAGE_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "Azure Storage connection string.", GROUP_AZURE, ++azureGroupCounter,
                ConfigDef.Width.NONE, AZURE_STORAGE_CONNECTION_STRING_CONFIG);

        configDef.define(AZURE_STORAGE_CONTAINER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                CONTAINER_NAME_VALIDATOR, ConfigDef.Importance.HIGH,
                "The Azure Blob container that files will be written to or read from.", GROUP_AZURE,
                ++azureGroupCounter, ConfigDef.Width.NONE, AZURE_STORAGE_CONTAINER_NAME_CONFIG);

        configDef.define(AZURE_PREFIX_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "Prefix for storage file names, generally specifies directory like"
                        + " structures that do not contain any templated fields.",
                GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.NONE, AZURE_PREFIX_CONFIG);
        if (!isSink) {
            configDef.define(AZURE_FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.MEDIUM, "Azure fetch page size", GROUP_AZURE, ++azureGroupCounter,
                    ConfigDef.Width.NONE, AZURE_FETCH_PAGE_SIZE);

            configDef.define(AZURE_FETCH_BUFFER_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.MEDIUM,
                    "Azure fetch buffer size. This is the number of object keys kept in a buffer to ensure lexically older objet keys aren't skipped for processing if they are slower to upload.",
                    GROUP_AZURE, ++azureGroupCounter, ConfigDef.Width.NONE, AZURE_FETCH_BUFFER_SIZE);
        }
    }

    static void addAzureRetryPolicies(final ConfigDef configDef) {
        int retryPolicyGroupCounter = 0;
        configDef.define(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Initial retry delay in milliseconds.", GROUP_AZURE_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter,
                ConfigDef.Width.NONE, AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Maximum retry delay in milliseconds.", GROUP_AZURE_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter,
                ConfigDef.Width.NONE, AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, ConfigDef.Type.INT,
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Retry max attempts. The default value is " + AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, ConfigDef.Width.NONE,
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public int getAzureFetchPageSize() {
        return getInt(AZURE_FETCH_PAGE_SIZE);
    }

    public String getAzurePrefix() {
        return getString(AZURE_PREFIX_CONFIG);
    }

    public String getConnectionString() {
        return getString(AZURE_STORAGE_CONNECTION_STRING_CONFIG);
    }

    public String getContainerName() {
        return getString(AZURE_STORAGE_CONTAINER_NAME_CONFIG);
    }

    public int getAzureRetryBackoffMaxAttempts() {
        return getInt(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public Duration getAzureRetryBackoffInitialDelay() {
        return Duration.ofMillis(getLong(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    public Duration getAzureRetryBackoffMaxDelay() {
        return Duration.ofMillis(getLong(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    public String getUserAgent() {
        return getString(AZURE_USER_AGENT);
    }

    public int getFetchBufferSize() {
        return getInt(AZURE_FETCH_BUFFER_SIZE);
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

}
