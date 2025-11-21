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

package io.aiven.kafka.connect.gcs;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.ConfigFragment;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

public final class GcsSinkConfigDef extends SinkCommonConfig.SinkCommonConfigDef {
    public static final String GCS_ENDPOINT_CONFIG = "gcs.endpoint";
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_CREDENTIALS_DEFAULT_CONFIG = "gcs.credentials.default";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    public static final String GCS_OBJECT_CONTENT_ENCODING_CONFIG = "gcs.object.content.encoding";
    public static final String GCS_USER_AGENT = "gcs.user.agent";
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "gcs.retry.backoff.initial.delay.ms";
    public static final String GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "gcs.retry.backoff.max.delay.ms";
    public static final String GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG = "gcs.retry.backoff.delay.multiplier";
    public static final String GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG = "gcs.retry.backoff.total.timeout.ms";
    public static final String GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "gcs.retry.backoff.max.attempts";
    // All default from GCS client, hardcoded here since GCS hadn't constants
    public static final long GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;
    public static final long GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;
    public static final double GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT = 2.0D;
    public static final long GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT = 50_000L;
    public static final int GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;
    private static final String GCS_GROUP = "GCS";
    private static final String GCS_GROUP_RETRY_BACKOFF_POLICY = "GCS retry backoff policy";
    private static final String USER_AGENT_HEADER_FORMAT = "Google GCS Sink/%s (GPN: Aiven;)";
    public static final String USER_AGENT_HEADER_VALUE = String.format(USER_AGENT_HEADER_FORMAT, Version.VERSION);

    public GcsSinkConfigDef() {
        super(OutputFieldType.VALUE, CompressionType.NONE);
        addGcsConfigGroup(this);
        addGcsRetryPolicies(this);
        addUserAgentConfig(this);
    }

    static void addGcsConfigGroup(final ConfigDef configDef) {
        int gcsGroupCounter = 0;
        configDef.define(GCS_ENDPOINT_CONFIG, Type.STRING, null, Importance.LOW,
                "Explicit GCS Endpoint Address, mainly for testing", GCS_GROUP, ++gcsGroupCounter, Width.NONE,
                GCS_ENDPOINT_CONFIG);
        configDef.define(GCS_CREDENTIALS_PATH_CONFIG, Type.STRING, null, Importance.LOW,
                "The path to a GCP credentials file. Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG
                        + " or \"" + GCS_CREDENTIALS_DEFAULT_CONFIG + "\"",
                GCS_GROUP, ++gcsGroupCounter, Width.NONE, GCS_CREDENTIALS_PATH_CONFIG);

        configDef.define(GCS_CREDENTIALS_JSON_CONFIG, Type.PASSWORD, null, Importance.LOW,
                "GCP credentials as a JSON string. Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG
                        + " or \"" + GCS_CREDENTIALS_DEFAULT_CONFIG + "\"",
                GCS_GROUP, ++gcsGroupCounter, Width.NONE, GCS_CREDENTIALS_JSON_CONFIG);

        configDef.define(GCS_CREDENTIALS_DEFAULT_CONFIG, Type.BOOLEAN, null, Importance.LOW,
                "Whether to connect using default the GCP SDK default credential discovery. When set to"
                        + "null (the default) or false, will fall back to connecting with No Credentials."
                        + "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\" or \""
                        + GCS_CREDENTIALS_PATH_CONFIG + "\"",
                GCS_GROUP, ++gcsGroupCounter, Width.NONE, GCS_CREDENTIALS_DEFAULT_CONFIG);

        configDef.define(GCS_OBJECT_CONTENT_ENCODING_CONFIG, Type.STRING, null, new NonEmptyString(), Importance.LOW,
                "The GCS object metadata value of Content-Encoding.", GCS_GROUP, ++gcsGroupCounter, Width.NONE,
                GCS_OBJECT_CONTENT_ENCODING_CONFIG);

        configDef.define(GCS_BUCKET_NAME_CONFIG, Type.STRING, NO_DEFAULT_VALUE, new NonEmptyString(), Importance.HIGH,
                "The GCS bucket name to store output files in.", GCS_GROUP, ++gcsGroupCounter, Width.NONE,
                GCS_BUCKET_NAME_CONFIG);
    }

    static void addGcsRetryPolicies(final ConfigDef configDef) {
        var retryPolicyGroupCounter = 0;
        configDef.define(GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, Type.LONG,
                GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT, Range.atLeast(0L), Importance.MEDIUM,
                "Initial retry delay in milliseconds. The default value is "
                        + GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT,
                GCS_GROUP_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, Width.NONE,
                GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, Type.LONG, GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                Range.atLeast(0L), Importance.MEDIUM,
                "Maximum retry delay in milliseconds. The default value is " + GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                GCS_GROUP_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, Width.NONE,
                GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG, Type.DOUBLE,
                GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT, Range.atLeast(1.0D), Importance.MEDIUM,
                "Retry delay multiplier. The default value is " + GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT,
                GCS_GROUP_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, Width.NONE,
                GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, Type.INT, GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                Range.atLeast(0L), Importance.MEDIUM,
                "Retry max attempts. The default value is " + GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                GCS_GROUP_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, Width.NONE,
                GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG, Type.LONG,
                GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT, ConfigDef.Range.between(0L, 86_400_000L), Importance.MEDIUM,
                "Retry total timeout in milliseconds.", GCS_GROUP_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter,
                Width.NONE, GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG);
    }

    static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(GCS_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting google");
    }

    @Override
    public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
        final Map<String, ConfigValue> values = super.multiValidate(valueMap);
        final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);

        final String errFmt = "if %s is set, %s may not be set";
        if (fragmentDataAccess.has(GCS_CREDENTIALS_PATH_CONFIG)
                && fragmentDataAccess.has(GCS_CREDENTIALS_JSON_CONFIG)) {
            ConfigFragment.registerIssue(valueMap, GCS_CREDENTIALS_PATH_CONFIG,
                    fragmentDataAccess.getString(GCS_CREDENTIALS_PATH_CONFIG),
                    String.format(errFmt, GCS_CREDENTIALS_PATH_CONFIG, GCS_CREDENTIALS_JSON_CONFIG));
        }
        if (fragmentDataAccess.has(GCS_CREDENTIALS_PATH_CONFIG)
                && fragmentDataAccess.has(GCS_CREDENTIALS_DEFAULT_CONFIG)) {
            ConfigFragment.registerIssue(valueMap, GCS_CREDENTIALS_PATH_CONFIG,
                    fragmentDataAccess.getString(GCS_CREDENTIALS_PATH_CONFIG),
                    String.format(errFmt, GCS_CREDENTIALS_PATH_CONFIG, GCS_CREDENTIALS_DEFAULT_CONFIG));
        }

        if (fragmentDataAccess.has(GCS_CREDENTIALS_JSON_CONFIG)
                && fragmentDataAccess.has(GCS_CREDENTIALS_DEFAULT_CONFIG)) {
            ConfigFragment.registerIssue(valueMap, GCS_CREDENTIALS_JSON_CONFIG,
                    fragmentDataAccess.getPassword(GCS_CREDENTIALS_JSON_CONFIG),
                    String.format(errFmt, GCS_CREDENTIALS_JSON_CONFIG, GCS_CREDENTIALS_DEFAULT_CONFIG));
        }

        return values;
    }
}
