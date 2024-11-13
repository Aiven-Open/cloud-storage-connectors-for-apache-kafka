package io.aiven.kafka.connect.common.config;

import org.apache.kafka.common.config.AbstractConfig;

public abstract class ConfigFragment {
    protected final AbstractConfig cfg;

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
}
