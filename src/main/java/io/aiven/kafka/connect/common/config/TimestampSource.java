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

package io.aiven.kafka.connect.common.config;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.sink.SinkRecord;

public interface TimestampSource {

    ZonedDateTime time(final SinkRecord record);

    static TimestampSource of(final Type extractorType) {
        return of(ZoneOffset.UTC, extractorType);
    }

    static TimestampSource of(final ZoneId zoneId, final Type extractorType) {
        switch (extractorType) {
            case WALLCLOCK:
                return new WallclockTimestampSource(zoneId);
            case EVENT:
                return new EventTimestampSource(zoneId);
            default:
                throw new IllegalArgumentException(
                    String.format("Unsupported timestamp extractor type: %s", extractorType)
                );
        }
    }

    Type type();

    enum Type {

        WALLCLOCK,
        EVENT;

        public static Type of(final String name) {
            for (final Type t : Type.values()) {
                if (t.name().equalsIgnoreCase(name)) {
                    return t;
                }
            }
            throw new IllegalArgumentException(String.format("Unknown timestamp source: %s", name));
        }

    }

    final class WallclockTimestampSource implements TimestampSource {
        private final ZoneId zoneId;

        protected WallclockTimestampSource(final ZoneId zoneId) {
            this.zoneId = zoneId;
        }

        @Override
        public ZonedDateTime time(final SinkRecord record) {
            return ZonedDateTime.now(zoneId);
        }

        public Type type() {
            return Type.WALLCLOCK;
        }
    }

    final class EventTimestampSource implements TimestampSource {
        private final ZoneId zoneId;

        protected EventTimestampSource(final ZoneId zoneId) {
            this.zoneId = zoneId;
        }

        @Override
        public ZonedDateTime time(final SinkRecord record) {
            return ZonedDateTime.ofInstant(
                    Instant.ofEpochMilli(record.timestamp()),
                    zoneId
            );
        }

        public Type type() {
            return Type.EVENT;
        }
    }
}
