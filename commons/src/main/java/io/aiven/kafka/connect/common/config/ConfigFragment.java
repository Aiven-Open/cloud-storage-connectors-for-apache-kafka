package io.aiven.kafka.connect.common.config;

import org.apache.kafka.common.config.AbstractConfig;

/**
 * Base for all configuration fragments.
 */
public abstract class ConfigFragment {
    /** The configuration that this fragment is associated with */
    protected final AbstractConfig cfg;

    /**
     * Construct the ConfigFragment..
     * @param cfg the configuration that this fragment is associated with.
     */
    protected ConfigFragment(AbstractConfig cfg) {
        this.cfg = cfg;
    }

    /**
     * Validate that the data in the configuration matches any restrictions.
     * Default implementation does nothing.
     */
    public void validate() {
        // does nothing
    }

    /**
     * Determines if a key has been set.
     * @param key The key to check.
     * @return {@code true} if the key was set, {@code false} if the key was not set or does not exist in the config.
     */
    public boolean has(String key) {
        return cfg.values().get(key) != null;
    }
}
