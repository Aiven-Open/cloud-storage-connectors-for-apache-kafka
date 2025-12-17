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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkConfig extends SinkCommonConfig {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkConfig.class);

    public GcsSinkConfig(final Map<String, String> properties) {
        super(new GcsSinkConfigDef(), properties);
    }

    public OAuth2Credentials getCredentials() {
        final String credentialsPath = getString(GcsSinkConfigDef.GCS_CREDENTIALS_PATH_CONFIG);
        final Password credentialsJsonPwd = getPassword(GcsSinkConfigDef.GCS_CREDENTIALS_JSON_CONFIG);
        final Boolean defaultCredentials = getBoolean(GcsSinkConfigDef.GCS_CREDENTIALS_DEFAULT_CONFIG);

        // if we've got no path, json and not configured to use default credentials, fall back to connecting without
        // any credentials at all.
        if (credentialsPath == null && credentialsJsonPwd == null
                && (defaultCredentials == null || !defaultCredentials)) {
            LOG.warn("No GCS credentials provided, trying to connect without credentials.");
            return NoCredentials.getInstance();
        }

        try {
            if (Boolean.TRUE.equals(defaultCredentials)) {
                return GoogleCredentials.getApplicationDefault();
            }

            String credentialsJson = null;
            if (credentialsJsonPwd != null) {
                credentialsJson = credentialsJsonPwd.value();
            }
            return GoogleCredentialsBuilder.build(credentialsPath, credentialsJson);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }

    public String getBucketName() {
        return getString(GcsSinkConfigDef.GCS_BUCKET_NAME_CONFIG);
    }

    public String getObjectContentEncoding() {
        return getString(GcsSinkConfigDef.GCS_OBJECT_CONTENT_ENCODING_CONFIG);
    }

    @Override
    public List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(OutputFormatFragment.FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE) {
                encodingType = outputFormatFragment.getOutputFieldEncodingType();
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }

    public int getGcsRetryBackoffMaxAttempts() {
        return getInt(GcsSinkConfigDef.GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public double getGcsRetryBackoffDelayMultiplier() {
        return getDouble(GcsSinkConfigDef.GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG);
    }

    public Duration getGcsRetryBackoffTotalTimeout() {
        return Duration.ofMillis(getLong(GcsSinkConfigDef.GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG));
    }

    public Duration getGcsRetryBackoffInitialDelay() {
        return Duration.ofMillis(getLong(GcsSinkConfigDef.GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    public Duration getGcsRetryBackoffMaxDelay() {
        return Duration.ofMillis(getLong(GcsSinkConfigDef.GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    public String getGcsEndpoint() {
        return getString(GcsSinkConfigDef.GCS_ENDPOINT_CONFIG);
    }

    public String getUserAgent() {
        return getString(GcsSinkConfigDef.GCS_USER_AGENT);
    }
}
