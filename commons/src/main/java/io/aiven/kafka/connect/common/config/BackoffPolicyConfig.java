package io.aiven.kafka.connect.common.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class BackoffPolicyConfig extends ConfigFragment {

    private static final String GROUP_RETRY_BACKOFF_POLICY = "Retry backoff policy";
    private static final String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";

    protected BackoffPolicyConfig(AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        configDef.define(KAFKA_RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, null, new ConfigDef.Validator() {

                    final long maximumBackoffPolicy = TimeUnit.HOURS.toMillis(24);

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (Objects.isNull(value)) {
                            return;
                        }
                        assert value instanceof Long;
                        final var longValue = (Long) value;
                        if (longValue < 0) {
                            throw new ConfigException(name, value, "Value must be at least 0");
                        } else if (longValue > maximumBackoffPolicy) {
                            throw new ConfigException(name, value,
                                    "Value must be no more than " + maximumBackoffPolicy + " (24 hours)");
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "The retry backoff in milliseconds. "
                        + "This config is used to notify Kafka Connect to retry delivering a message batch or "
                        + "performing recovery in case of transient exceptions. Maximum value is "
                        + TimeUnit.HOURS.toMillis(24) + " (24 hours).",
                GROUP_RETRY_BACKOFF_POLICY, 1, ConfigDef.Width.NONE, KAFKA_RETRY_BACKOFF_MS_CONFIG);

        return configDef;
    }

    public Long getKafkaRetryBackoffMs() {
        return cfg.getLong(KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }
}
