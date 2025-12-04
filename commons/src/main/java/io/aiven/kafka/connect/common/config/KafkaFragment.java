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

package io.aiven.kafka.connect.common.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;

/**
 * Defines the configuration for Kafka. This fragment only has the Setter and a few constants for Connectors.
 */
public final class KafkaFragment extends ConfigFragment {

    /**
     * Enum for setting discovery.
     */
    public enum PluginDiscovery {
        ONLY_SCAN, SERVICE_LOAD, HYBRID_WARN, HYBRID_FAIL
    }

    /**
     * The configuration string for plugin discovery.
     */
    private final static String PLUGIN_DISCOVERY = "plugin.discovery";

    public KafkaFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Get the setter for this fragment.
     *
     * @param data
     *            The data map for the configuration.
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * The setter class to set options in a data map for the configuration.
     */
    @SuppressWarnings("PMD.TooManyMethods")
    public final static class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructor.
         *
         * @param data
         *            the map of data to update.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the offset commit flush interval.
         *
         * @param interval
         *            the interval between flushes.
         * @return this.
         */
        public Setter offsetFlushInterval(final Duration interval) {
            return setValue(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, interval.toMillis());
        }

        /**
         * Sets the offset commit timeout.
         *
         * @param interval
         *            the interval between commit timeouts.
         * @return this.
         */
        public Setter offsetTimeout(final Duration interval) {
            return setValue(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, interval.toMillis());
        }

        /**
         * Globally unique name to use for this connector.
         *
         * @param name
         *            Globally unique name to use for this connector.
         * @return this
         */
        public Setter name(final String name) {
            return setValue(ConnectorConfig.NAME_CONFIG, name);
        }

        /**
         * Sets the plugin discovery strategy.
         *
         * @param pluginDiscovery
         *            the plugin discovery strategy.
         * @return this
         */
        public Setter pluginDiscovery(final PluginDiscovery pluginDiscovery) {
            return setValue(PLUGIN_DISCOVERY, pluginDiscovery.name());
        }

        /**
         * Sets the maximum number of tasks for the connector.
         *
         * @param tasksMax
         *            the maximum number of tasks.
         * @return this
         */
        public Setter tasksMax(final int tasksMax) {
            return setValue(ConnectorConfig.TASKS_MAX_CONFIG, tasksMax);
        }

        /**
         * Sets the list of transforms
         *
         * @param transforms
         *            a comma separated list of transforms.
         * @return this
         */
        public Setter transforms(final String transforms) {
            return setValue(ConnectorConfig.TRANSFORMS_CONFIG, transforms);
        }

        /**
         * Sets the bootstrap servers.
         *
         * @param bootstrapServers
         *            the bootstrap servers.
         * @return this
         */
        public Setter bootstrapServers(final String bootstrapServers) {
            return setValue(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }

        /**
         * Sets the task shutdown timeout.
         *
         * @param timeout
         *            the timeout.
         * @return this
         */
        public Setter taskShutdownTimeout(final Duration timeout) {
            return setValue(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, timeout.toMillis());
        }

        /**
         * Sets the configuration listeners.
         *
         * @param listeners
         *            a comma separated string of configuration listeners.
         * @return this
         */
        public Setter listeners(final String listeners) {
            return setValue(WorkerConfig.LISTENERS_CONFIG, listeners);
        }

        /**
         * Sets the configuration listeners.
         *
         * @param listeners
         *            an array of configuration listeners.
         * @return this
         */
        public Setter listeners(final String... listeners) {
            return setValue(WorkerConfig.LISTENERS_CONFIG, String.join(", ", listeners));
        }

        /**
         * Sets the advertised host name.
         *
         * @param hostName
         *            the advertised host name
         * @return this
         */
        public Setter advertisedHostName(final String hostName) {
            return setValue(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, hostName);
        }

        /**
         * Sets the advertised host port.
         *
         * @param port
         *            the advertised host port.
         * @return
         */
        public Setter advertisedHostPort(final int port) {
            return setValue(WorkerConfig.REST_ADVERTISED_PORT_CONFIG, port);
        }

        /**
         * Sets the advertised listener protocol.
         *
         * @param protocol
         *            HTTP or HTTPS
         * @return this
         */
        public Setter advertisedListenerProtocol(final String protocol) {
            return setValue(WorkerConfig.REST_ADVERTISED_LISTENER_CONFIG, protocol);
        }

        /**
         * Sets the aAccess-Control-Allow-Origin header to for REST API requests. To enable cross-origin access, set
         * this to the domain of the application that should be permitted to access the API, or '*' to allow access from
         * any domain. The default value only allows access from the domain of the REST API.
         *
         * @param origin
         *            the orgin to allow
         * @return this
         */
        public Setter accessControlAllowOrigin(final String origin) {
            return setValue(WorkerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, origin);
        }

        /**
         * Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header. The
         * default value of the Access-Control-Allow-Methods header allows cross-origin requests for GET, POST and HEAD.
         *
         * @param methods
         *            A list of methods to allow.
         * @return this
         */
        public Setter accessControlAllowMethods(final String methods) {
            return setValue(WorkerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, methods);
        }

        /**
         * Sets the list of paths separated by commas (,) that contain plugins (connectors, converters,
         * transformations). Example: "/usr/local/share/java,/usr/local/share/kafka/plugins,/opt/connectors"
         *
         * @param pluginPath
         *            the list of paths.
         * @return this
         * @see #pluginPath(String...)
         */
        public Setter pluginPath(final String pluginPath) {
            return setValue(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath);
        }

        /**
         * Sets the list of paths that contain plugins (connectors, converters, transformations). The list should
         * consist of top level directories that include any combination of:
         * <ul>
         * <li>directories immediately containing jars with plugins and their dependencies</li>
         * <li>uber-jars with plugins and their dependencies</li>
         * <li>directories immediately containing the package directory structure of classes of plugins and their
         * dependencies</li>
         * </ul>
         * <p>
         * Note: symlinks will be followed to discover dependencies or plugins.
         * </p>
         *
         * @param pluginPath
         *            the list of paths to to search,.
         * @return this.
         */
        public Setter pluginPath(final String... pluginPath) {
            return setValue(WorkerConfig.PLUGIN_PATH_CONFIG, String.join(", ", pluginPath));
        }

        /**
         * Sets the metrics sample window size.
         *
         * @param window
         *            the time for each window.
         * @return this
         */
        public Setter metricsSampleWindow(final Duration window) {
            return setValue(WorkerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, window.toMillis());
        }

        /**
         * The number of samples maintained to compute metrics.
         *
         * @param count
         *            the number of samples.
         * @return this.
         */
        public Setter metricsSampleCount(final int count) {
            return setValue(WorkerConfig.METRICS_NUM_SAMPLES_CONFIG, count);
        }

        /**
         * The highest recording level for metrics.
         *
         * @param level
         *            the recording level
         * @return this.
         */
        public Setter metricsRecordingLevel(final int level) {
            return setValue(WorkerConfig.METRICS_RECORDING_LEVEL_CONFIG, level);
        }

        /**
         * A list of classes to use as metrics reporters. Implementing the
         * {@link org.apache.kafka.common.metrics.MetricsReporter} interface allows plugging in classes that will be
         * notified of new metric creation. The JmxReporter is always included to register JMX statistics.
         *
         * @param reporterClasses
         *            the classes to use.
         * @return this
         */
        public Setter metricReporterClasses(final Class<? extends MetricsReporter>... reporterClasses) {
            return setValue(WorkerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    Arrays.stream(reporterClasses).map(Class::getName).collect(Collectors.joining(", ")));
        }
    }
}
