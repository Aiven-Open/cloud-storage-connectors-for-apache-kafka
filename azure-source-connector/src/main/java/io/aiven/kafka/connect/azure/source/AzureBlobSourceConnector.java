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

package io.aiven.kafka.connect.azure.source;

import static io.aiven.kafka.connect.common.config.CommonConfig.TASK_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.utils.VersionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AzureBlobSourceConnector is a Kafka Connect Connector implementation that watches an azure blob container and
 * generates tasks to ingest contents.
 */
public class AzureBlobSourceConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceConnector.class);

    private Map<String, String> configProperties;

    @Override
    public ConfigDef config() {
        return AzureBlobSourceConfig.configDef();
    }

    @Override
    public String version() {
        return new VersionInfo().getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AzureBlobSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final var taskProps = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            final var props = new HashMap<>(configProperties); // NOPMD
            props.put(TASK_ID, String.valueOf(i));
            taskProps.add(props);
        }
        return taskProps;
    }

    @Override
    public void start(final Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties haven't been set");
        configProperties = Map.copyOf(properties);
        LOGGER.info("Start Azure Source connector");
    }

    @Override
    public void stop() {
        LOGGER.info("Stop Azure Source connector");
    }
}
