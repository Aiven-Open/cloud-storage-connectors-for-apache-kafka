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

package io.aiven.kafka.connect.s3;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

public final class OldFullKeyFormatters {
    public static final BiFunction<SinkRecord, VariableTemplatePart.Parameter, String> KAFKA_OFFSET = (sinkRecord,
            usePaddingParameter) -> usePaddingParameter.asBoolean()
                    ? String.format("%020d", sinkRecord.kafkaOffset())
                    : Long.toString(sinkRecord.kafkaOffset());

    private OldFullKeyFormatters() {
        /* hide constructor */ }

}
