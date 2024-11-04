package io.aiven.kafka.connect.gcs;

import io.aiven.kafka.connect.common.config.validators.ClassValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

public class AsyncGcsSinkConfig extends GcsSinkConfig{

    private static final String GROUP_ASYNC = "Async";
    private static final String ASYNC_MAX_RECORD_AGE = "async.file.max.record.age.ms";
    private static final String ASYNC_MAX_OPEN_FILES = "async.file.max.open.files";
    private static final String ASYNC_TASK_CLASS = "async.task.class";

    public AsyncGcsSinkConfig(Map<String, String> properties) {
        super(configDef(), properties);
    }

    public static ConfigDef configDef() {
        final ConfigDef configDef = GcsSinkConfig.configDef();
        addAsyncConfig(configDef);
        return configDef;
    }
    private static void addAsyncConfig(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(ASYNC_MAX_RECORD_AGE, ConfigDef.Type.INT, 60000, ConfigDef.Importance.LOW,
                "write files asynchronously", GROUP_ASYNC, groupCounter++, ConfigDef.Width.NONE, ASYNC_MAX_RECORD_AGE);
        configDef.define(ASYNC_MAX_OPEN_FILES, ConfigDef.Type.INT, 100, ConfigDef.Importance.LOW,
                "write files asynchronously", GROUP_ASYNC, groupCounter++, ConfigDef.Width.NONE, ASYNC_MAX_OPEN_FILES);
        configDef.define(ASYNC_TASK_CLASS, ConfigDef.Type.CLASS,  AsyncGcsSinkTask.class, new ClassValidator(AsyncGcsSinkTask.class),
                ConfigDef.Importance.LOW,"the task class", GROUP_ASYNC, groupCounter++, ConfigDef.Width.NONE, ASYNC_TASK_CLASS);
    }


    public int getAsyncMaxRecordAgeMs() {
        return getInt(ASYNC_MAX_RECORD_AGE);
    }

    public Class<? extends Task> getTaskClass() {
        return getClass(ASYNC_MAX_RECORD_AGE).asSubclass(AsyncGcsSinkTask.class);
    }
}
