/*
 * Copyright 2022 Aiven Oy
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

package io.aiven.kafka.connect.common.compatibility;

import java.util.concurrent.Future;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import io.aiven.kafka.connect.common.compatibility.features.Features;

/**
 * This interface is a shim for the {@link org.apache.kafka.connect.sink.ErrantRecordReporter} interface that
 * developers can use in their connectors without having to sacrifice compatibility with older versions of the Kafka
 * Connect framework or deal with reflection errors.
 * @see Features#SINK_TASK_ERRANT_RECORD_REPORTER
 */
public interface ErrantRecordReporter {

    /**
     * @param record the invalid record
     * @param error the error with the record
     * @return the result of reporting the error with the record
     * @see org.apache.kafka.connect.sink.ErrantRecordReporter#report(SinkRecord, Throwable)
     */
    Future<Void> report(SinkRecord record, Throwable error);

    /**
     * Get a reporter that can be used for this connector. If the runtime does not support the
     * {@link org.apache.kafka.connect.sink.ErrantRecordReporter} interface, or if the user has not configured this
     * connector to use an error reporter, returns a default implementation whose
     * {@link #report(SinkRecord, Throwable)} method always throws an exception.
     * @param context the {@link SinkTaskContext} that the task has been instantiated with
     * @return a reporter that can be used by the task; never null
     */
    static ErrantRecordReporter reporter(final SinkTaskContext context) {
        if (Features.SINK_TASK_ERRANT_RECORD_REPORTER.supported()) {
            final org.apache.kafka.connect.sink.ErrantRecordReporter userConfigured = context.errantRecordReporter();
            return userConfigured != null
                    ? new SupportedErrantRecordReporter(userConfigured)
                    : new DisabledErrantRecordReporter(true);
        } else {
            return new DisabledErrantRecordReporter(false);
        }
    }

}
