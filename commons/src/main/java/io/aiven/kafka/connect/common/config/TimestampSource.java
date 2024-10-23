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
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.extractors.DataExtractor;
import io.aiven.kafka.connect.common.config.extractors.HeaderValueExtractor;
import io.aiven.kafka.connect.common.config.extractors.SimpleValuePath;

public interface TimestampSource {

    ZonedDateTime time(SinkRecord record);

    Type type();

    enum Type {

        WALLCLOCK, //
        EVENT, //
        HEADER, //
        SIMPLE_DATA, //
        CUSTOM//

    }
    class Builder {
        private ZoneId zoneId = ZoneOffset.UTC;
        private Type type;
        private String additionalParameters;

        /**
         * set the zoneId to be used. If this method isnt called, the default is UTC
         *
         * @return this
         * @throws NullPointerException
         *             if zoneId is null
         */
        public Builder zoneId(final ZoneId zoneId) {
            Objects.requireNonNull(zoneId, "zoneId cannot be null");
            this.zoneId = zoneId;
            return this;
        }

        /**
         * sets the type of the timestamp source and associated parameters (if needed) The format of the configuration
         * is &lt;type&gt;[:&lt;data&gt;] i.e. the type name, optionally followed by data. <br>
         * The data is type specific
         * <p>
         * For type WALLCLOCK or EVENT, no data is allowed
         * </p>
         * <p>
         * For type SIMPLE_DATA, data is required, and is a '.' separated series of terms in the path <br>
         * If the '.' is something that should be included in the terms, and you want to use a different separator, then
         * you can specify a '.' as the first character, and the separator as the second character, and then the path is
         * the rest of the string <br>
         * For example "SIMPLE_DATA:a.b.c" would use into a path with terms "a", "b", "c" <br>
         * For example "SIMPLE_DATA:.:a.b:c" would use a path with terms "a.b", "c"
         * <p>
         * For type HEADER, data is required, and is the name of the header to extract <br>
         * For example "HEADER:foo" would use to "foo" header (or null if its not available in the SinkRecord
         * </p>
         * <p>
         * For type CUSTOM, data is required, and is the name of the class to use, and any additional parameters for
         * that custom time source. The specified class must implement the TimestampSource interface and have a public
         * constructor that takes a String and a ZoneId. Fort the meaning of the data, see the documentation of the
         * custom class. <br>
         * For example "CUSTOM:my.custom.timesource:some more data" would be similar to calling new
         * my.custom.timesource("some more data", zoneId)
         * </p>
         *
         *
         * @return this
         */
        public Builder configuration(final String configuration) {
            final String[] parts = configuration.split(":", 2);
            final String typeName = parts[0];
            try {
                this.type = Type.valueOf(typeName.toUpperCase(Locale.ENGLISH));
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown timestamp source: " + typeName);
            }

            this.additionalParameters = parts.length > 1 ? parts[1] : null;
            return this;
        }

        public TimestampSource build() {
            switch (type) {
                case WALLCLOCK :
                    if (additionalParameters != null) {
                        throw new IllegalArgumentException(
                                "Wallclock timestamp source does not support additionalParameters");
                    }
                    return new WallclockTimestampSource(zoneId);
                case EVENT :
                    if (additionalParameters != null) {
                        throw new IllegalArgumentException(
                                "Event timestamp source does not support additionalParameters");
                    }
                    return new EventTimestampSource(zoneId);
                case SIMPLE_DATA :
                    if (additionalParameters == null) {
                        throw new IllegalArgumentException("Data timestamp source requires additionalParameters");
                    }
                    return new SimpleTimestampSource(zoneId, Type.SIMPLE_DATA,
                            SimpleValuePath.parse(additionalParameters));
                case HEADER :
                    if (additionalParameters == null) {
                        throw new IllegalArgumentException("Header timestamp source requires additionalParameters");
                    }
                    return new SimpleTimestampSource(zoneId, Type.HEADER,
                            new HeaderValueExtractor(additionalParameters));
                case CUSTOM :
                    if (additionalParameters == null) {
                        throw new IllegalArgumentException("Header timestamp source requires additionalParameters");
                    }
                    final String[] parts = additionalParameters.split(":", 2);
                    final String className = parts[0];
                    final String params = parts.length > 1 ? parts[1] : null;
                    try {
                        final Class<?> clazz = Class.forName(className);
                        return (TimestampSource) clazz.getConstructor(String.class, ZoneId.class)
                                .newInstance(params, zoneId);
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Failed to create custom timestamp source", e);
                    }
                default :
                    throw new IllegalArgumentException(String.format("Unsupported timestamp extractor type: %s", type));
            }
        }

    }

    class SimpleTimestampSource implements TimestampSource {
        protected final ZoneId zoneId;
        private final Type type;
        private final DataExtractor dataExtractor;

        protected SimpleTimestampSource(final ZoneId zoneId, final Type type, DataExtractor dataExtractor) {
            this.zoneId = zoneId;
            this.type = type;
            this.dataExtractor = dataExtractor;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public ZonedDateTime time(SinkRecord record) {
            return fromRawTime(dataExtractor.extractDataFrom(record));
        }

        protected ZonedDateTime fromRawTime(final Object rawValue) {
            if (rawValue == null) {
                return null;
            } else if (rawValue instanceof Long) {
                return withZone((Long) rawValue);
            } else if (rawValue instanceof Date) {
                return withZone(((Date) rawValue).getTime());
            } else if (rawValue instanceof ZonedDateTime) {
                return (ZonedDateTime) rawValue;
            } else if (rawValue instanceof Instant) {
                return withZone(((Instant) rawValue).toEpochMilli());
            }
            return null;
        }

        protected ZonedDateTime withZone(final long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
        }
    }

    final class WallclockTimestampSource extends SimpleTimestampSource {
        WallclockTimestampSource(final ZoneId zoneId) {
            super(zoneId, Type.WALLCLOCK, null);
        }

        @Override
        public ZonedDateTime time(final SinkRecord record) {
            return ZonedDateTime.now(zoneId);
        }
    }

    final class EventTimestampSource extends SimpleTimestampSource {
        EventTimestampSource(final ZoneId zoneId) {
            super(zoneId, Type.EVENT, null);
        }

        @Override
        public ZonedDateTime time(final SinkRecord record) {
            return withZone(record.timestamp());
        }
    }
}
