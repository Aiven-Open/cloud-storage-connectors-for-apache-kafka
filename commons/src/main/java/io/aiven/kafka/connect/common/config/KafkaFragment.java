package io.aiven.kafka.connect.common.config;

import org.apache.kafka.connect.storage.Converter;

import java.time.Duration;
import java.util.Map;

public class KafkaFragment {

    public enum PluginDiscovery {ONLY_SCAN, SERVICE_LOAD, HYBRID_WARN, HYBRID_FAIL};


    public final static String VALUE_CONVERTER_KEY = "value.converter";
    public final static String KEY_CONVERTER_KEY = "key.converter";
    private final static String NAME = "name";
    private final static String INTERNAL_KEY_CONVERTER = "internal.key.converter";
    private final static String INTERNAL_KEY_CONVERTER_ENABLE = "internal.key.converter.enable";
    private final static String INTERNAL_VALUE_CONVERTER = "internal.value.converter";
    private final static String INTERNAL_VALUE_CONVERTER_ENABLE = "internal.value.converter";
    private final static String PLUGIN_DISCOVERY = "plugin.discovery";
    private final static String OFFSET_FLUSH_INTERVAL = "offset.flush.interval.ms";

    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    public final static class Setter extends AbstractFragmentSetter<Setter> {
        private Setter(Map<String, String> data) {
            super(data);
        }

        public Setter keyConverter(String converter) {
            return setValue(KEY_CONVERTER_KEY, converter);
        }

        public Setter keyConverter(Class<? extends Converter> converter) {
            return setValue(KEY_CONVERTER_KEY, converter);
        }
        public Setter valueConverter(String converter) {
            return setValue(VALUE_CONVERTER_KEY, converter);
        }

        public Setter valueConverter(Class<? extends Converter> converter) {
            return setValue(VALUE_CONVERTER_KEY, converter);
        }

        public Setter internalValueConverter(Class<? extends Converter> converter) {
            return setValue(INTERNAL_VALUE_CONVERTER, converter);
        }

        public Setter internalValueConverterEnable(boolean enable) {
            return setValue(INTERNAL_VALUE_CONVERTER_ENABLE, enable);
        }

        public Setter internalKeyConverter(Class<? extends Converter> converter) {
            return setValue(INTERNAL_KEY_CONVERTER, converter);
        }

        public Setter internalKeyConverterEnable(boolean enable) {
            return setValue(INTERNAL_KEY_CONVERTER_ENABLE, enable);
        }

        public Setter pluginDiscovery(PluginDiscovery pluginDiscovery) {
            return setValue(PLUGIN_DISCOVERY, pluginDiscovery.name());
        }

        public Setter offsetFlushInterval(Duration interval) {
            return setValue(OFFSET_FLUSH_INTERVAL, interval.toMillis());
        }

        public Setter name(String name) {
            return setValue(NAME, name);
        }

    }

}
