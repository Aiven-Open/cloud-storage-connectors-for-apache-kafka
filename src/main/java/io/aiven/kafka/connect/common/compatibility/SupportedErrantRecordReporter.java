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

import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * An {@link ErrantRecordReporter} that simply delegates all calls to an
 * {@link org.apache.kafka.connect.sink.ErrantRecordReporter}, which should be provided by the Kafka Connect runtime.
 */
class SupportedErrantRecordReporter implements ErrantRecordReporter {

    private final org.apache.kafka.connect.sink.ErrantRecordReporter reporter;

    public SupportedErrantRecordReporter(final org.apache.kafka.connect.sink.ErrantRecordReporter reporter) {
        this.reporter = Objects.requireNonNull(reporter);
    }

    @Override
    public Future<Void> report(final SinkRecord record, final Throwable error) {
        return reporter.report(record, error);
    }

}
