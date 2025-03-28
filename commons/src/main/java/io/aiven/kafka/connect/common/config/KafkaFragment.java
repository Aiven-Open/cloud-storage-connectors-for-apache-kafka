package io.aiven.kafka.connect.common.config;

import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaFragment {

    public enum PluginDiscovery {ONLY_SCAN, SERVICE_LOAD, HYBRID_WARN, HYBRID_FAIL};


    private final static String PLUGIN_DISCOVERY = "plugin.discovery";
    private final static String INTERNAL_VALUE_CONVERTER_SCHEMA_ENABLE = "internal.value.converter.schema.enable";
    private final static String INTERNAL_KEY_CONVERTER_SCHEMA_ENABLE = "internal.key.converter.schema.enable";

    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    public final static class Setter extends AbstractFragmentSetter<Setter> {
        private Setter(Map<String, String> data) {
            super(data);
        }

        /**
         * THe class for the connector.
         * @param connectorClass the class for the connector.
         * @return this
         */
        public Setter connector(Class<? extends Connector> connectorClass) {
            return setValue(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);
        }

        public Setter keyConverter(String converter) {
            return setValue(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter keyConverter(Class<? extends Converter> converter) {
            return setValue(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter valueConverter(String converter) {
            return setValue(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter valueConverter(Class<? extends Converter> converter) {
            return setValue(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter headerConverter(String header) {
            return setValue(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, header);
        }

        public Setter headerConverter(Class<? extends Converter> header) {
            return setValue(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, header);
        }

        public Setter internalValueConverter(Class<? extends Converter> converter) {
            return setValue(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter internalValueConverterSchemaEnable(boolean state) {
            return setValue(INTERNAL_VALUE_CONVERTER_SCHEMA_ENABLE, state);
        }


        public Setter internalKeyConverter(Class<? extends Converter> converter) {
            return setValue(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, converter);
        }

        public Setter internalKeyConverterSchemaEnable(boolean state) {
            return setValue(INTERNAL_KEY_CONVERTER_SCHEMA_ENABLE, state);
        }

        public Setter offsetFlushInterval(Duration interval) {
            return setValue(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, interval.toMillis());
        }

        public Setter offsetTimeout(Duration interval) {
            return setValue(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, interval.toMillis());
        }


        /**
         * Globally unique name to use for this connector.
         * @param name Globally unique name to use for this connector.
         * @return this
         */
        public Setter name(String name) {
            return setValue(ConnectorConfig.NAME_CONFIG, name);
        }

        public Setter pluginDiscovery(PluginDiscovery pluginDiscovery) {
            return setValue(PLUGIN_DISCOVERY, pluginDiscovery.name());
        }

        public Setter tasksMax(int tasksMax) {
            return setValue(ConnectorConfig.TASKS_MAX_CONFIG, tasksMax);
        }

        public Setter transforms(String transforms) {
            return setValue(ConnectorConfig.TRANSFORMS_CONFIG, transforms);
        }

        public Setter bootstrapServers(String bootstrapServers) {
            return setValue(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }

        public Setter taskShutdownTimeout(Duration timeout) {
            return setValue(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, timeout.toMillis());
        }

        public Setter listeners(String listeners) {
            return setValue(WorkerConfig.LISTENERS_CONFIG, listeners);
        }

        public Setter listeners(String... listeners) {
            return setValue(WorkerConfig.LISTENERS_CONFIG, String.join(", ", listeners));
        }

        public Setter advertisedHostName(String hostName) {
            return setValue(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, hostName);
        }

        public Setter advertisedHostPort(int port) {
            return setValue(WorkerConfig.REST_ADVERTISED_PORT_CONFIG, port);
        }

        /**
         * Sets the advertised listener protocal
         *
         * @param protocol HTTP or HTTPS
         * @return this
         */
        public Setter advertisedListenerProtocol(String protocol) {
            return setValue(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, protocol);
        }

        public Setter accessControlAllowOrigin(String origin) {
            return setValue(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, origin);
        }

        /**
         *  Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header.
         *  The default value of the Access-Control-Allow-Methods header allows cross origin requests for GET, POST and HEAD.
         * @param methods A list of methods to allow.
         * @return this
         */
        public Setter accessControlAllowMethods(String methods) {
            return setValue(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, methods);
        }

        /**
         * List of paths separated by commas (,) that contain plugins (connectors, converters, transformations).
         * Example: "/usr/local/share/java,/usr/local/share/kafka/plugins,/opt/connectors"
         * @param pluginPath the list of paths.
         * @return this
         * @see #pluginPath(String...)
         */
        public Setter pluginPath(String pluginPath) {
            return setValue(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath);
        }

        /**
         * List of paths that contain plugins (connectors, converters, transformations). The list should consist
         * of top level directories that include any combination of:
         * <ul>
         * <li>directories immediately containing jars with plugins and their dependencies</li>>
         *             <li>uber-jars with plugins and their dependencies</li>
         *             <li>directories immediately containing the package directory structure of classes of plugins and their dependencies</li>>
         *             </ul>
         * <p>Note: symlinks will be followed to discover dependencies or plugins.</p>
         * @param pluginPath the list of paths to to search,.
         * @return this.
         */
        public Setter pluginPath(String... pluginPath) {
            return setValue(WorkerConfig.PLUGIN_PATH_CONFIG, String.join(", ", pluginPath));
        }

        public Setter metricsSampleWindow(Duration window) {
            return setValue(WorkerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, window.toMillis());
        }

        /**
         * The number of samples maintained to compute metrics.
         * @param count the number of samples.
         * @return this.
         */
        public Setter metricsSampleCount(int count) {
            return setValue(WorkerConfig.METRICS_NUM_SAMPLES_CONFIG, count);
        }

        /**
         * The highest recording level for metrics.
         * @param level the recording level
         * @return this.
         */
        public Setter metricsRecordingLevel(int level) {
            return setValue(WorkerConfig.METRICS_RECORDING_LEVEL_CONFIG, level);
        }

        /**
         * A list of classes to use as metrics reporters. Implementing the {@link org.apache.kafka.common.metrics.MetricsReporter} interface allows plugging in classes that will be notified of new metric creation.
         * The JmxReporter is always included to register JMX statistics.
         * @param reporterClasses the classes to use.
         * @return this
         */
        public Setter metricReporterClasses(Class<? extends MetricsReporter>... reporterClasses ) {
            return setValue(WorkerConfig.METRIC_REPORTER_CLASSES_CONFIG, Arrays.stream(reporterClasses).map(Class::getName).collect(Collectors.joining(", ")));
        }
    }

}
