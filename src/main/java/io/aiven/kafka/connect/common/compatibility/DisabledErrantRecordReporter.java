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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * An {@link ErrantRecordReporter} that always throws an exception from {@link #report(SinkRecord, Throwable)}, and
 * provides a helpful message to the user containing instructions on how to enable the errant record reporter depending
 * on whether it is unsupported by the Kafka Connect runtime, or supported but not enabled by the user.
 */
class DisabledErrantRecordReporter implements ErrantRecordReporter {

    private final boolean supported;

    public DisabledErrantRecordReporter(final boolean supported) {
        this.supported = supported;
    }

    @Override
    public Future<Void> report(final SinkRecord record, final Throwable error) {
        final String preamble = String.format(
                "An error was encountered with the record from topic/partition/offset %s/%s/%s",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset()
        );
        final String message;
        if (supported) {
            message = preamble + ". Consider enabling the \"errors.tolerance\" property in your connector "
                    + "configuration if you would like to skip invalid records such as this one.";
        } else {
            message = preamble + ", "
                    + "and the version of Kafka Connect that this connector is deployed onto does not support "
                    + "the errant record reporter feature. Consider upgrading to version 2.6.0 or later if you would "
                    + "like to skip invalid records such as this one.";
        }
        throw new ConnectException(message, error);
    }

}
