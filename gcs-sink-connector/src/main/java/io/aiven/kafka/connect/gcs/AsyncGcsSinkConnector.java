package io.aiven.kafka.connect.gcs;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

public class AsyncGcsSinkConnector extends GcsSinkConnector {
    private AsyncGcsSinkConfig config;
    @Override
    public Class<? extends Task> taskClass() {
        return config.getTaskClass();
    }

    protected void setConfig(AsyncGcsSinkConfig config) {
        this.config = config ;
        super.setConfig(config);
    }

    @Override
    protected void parseConfig(Map<String, String> props) {
        setConfig(new AsyncGcsSinkConfig(props));
    }

    @Override
    public ConfigDef config() {
        return AsyncGcsSinkConfig.configDef();
    }
}
