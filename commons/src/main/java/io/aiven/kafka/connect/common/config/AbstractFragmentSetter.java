package io.aiven.kafka.connect.common.config;

import java.net.URI;
import java.util.Map;

public class AbstractFragmentSetter<T extends AbstractFragmentSetter<T>> {

    private final Map<String, String> data;

    protected AbstractFragmentSetter(final Map<String, String> data) {
        this.data = data;
    }

    final public Map<String, String> data() {
        return data;
    }

    final protected T setValue(final String key, final String value) {
        data.put(key, value);
        return (T) this;
    }

    final protected T setValue(final String key, final int value) {
        return setValue(key, Integer.toString(value));
    }

    final protected T setValue(final String key, final long value) {
        return setValue(key, Long.toString(value));
    }

    final protected T setValue(final String key, final Class<?> value) {
        return setValue(key, value.getCanonicalName());
    }

    final protected T setValue(final String key, final URI value) {
        return setValue(key, value.toString());
    }

    final protected T setValue(final String key, final boolean state) {
        return setValue(key, Boolean.toString(state));
    }
}
