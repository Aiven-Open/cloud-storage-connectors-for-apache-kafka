/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.compatibility.features;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTaskContext;

/**
 * This class allows developers to easily check at any point in the lifetime of a Kafka Connect plugin (connector,
 * converter, etc.) whether features of the Kafka Connect runtime that the plugin has been deployed onto are supported.
 */
public interface Features {

    /**
     * @see SinkTask#preCommit(Map)
     */
    Feature SINK_TASK_PRE_COMMIT = new MethodFeature() {
        @Override
        protected Class<?> klass() {
            return SinkTask.class;
        }

        @Override
        protected String method() {
            return "preCommit";
        }

        @Override
        protected List<Class<?>> parameters() {
            return Arrays.asList(Map.class);
        }

        @Override
        public String toString() {
            return "Sink task preCommit";
        }
    };

    /**
     * @see SinkTaskContext#errantRecordReporter()
     */
    Feature SINK_TASK_ERRANT_RECORD_REPORTER = new MethodFeature() {
        @Override
        protected Class<?> klass() {
            return SinkTaskContext.class;
        }

        @Override
        protected String method() {
            return "errantRecordReporter";
        }

        @Override
        protected List<Class<?>> parameters() {
            return Arrays.asList();
        }

        @Override
        public String toString() {
            return "Sink task errant record reporter";
        }
    };

    /**
     * @see SourceConnector#exactlyOnceSupport(Map)
     * @see SourceTaskContext#transactionContext()
     */
    Feature EXACTLY_ONCE_SOURCE_CONNECTORS = new MethodFeature() {
        @Override
        protected Class<?> klass() {
            return SourceConnector.class;
        }

        @Override
        protected String method() {
            return "exactlyOnceSupport";
        }

        @Override
        protected List<Class<?>> parameters() {
            return Arrays.asList(Map.class);
        }

        @Override
        public String toString() {
            return "Exactly-once source connectors";
        }
    };

}
