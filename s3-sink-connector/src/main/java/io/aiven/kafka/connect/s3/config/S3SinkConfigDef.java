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

package io.aiven.kafka.connect.s3.config;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.common.config.DataStorageUnit;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.utils.Size;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import static io.aiven.kafka.connect.common.config.FileNameFragment.GROUP_FILE;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.GROUP_AWS;

public class S3SinkConfigDef extends ConfigDef {

    public static final int DEFAULT_PART_SIZE = (int) DataStorageUnit.MEBIBYTES.toBytes(5);

    public S3SinkConfigDef() {
        super();
        S3ConfigFragment.update(this);
        addS3partSizeConfig(this);
        FileNameFragment.update(this);
        OutputFormatFragment.update(this, null);
        addDeprecatedTimestampConfig(this);
    }

    private static void addS3partSizeConfig(final ConfigDef configDef) {

        // add awsS3SinkCounter if more S3 Sink Specific config is added
        // This is used to set orderInGroup
        configDef.define(S3ConfigFragment.AWS_S3_PART_SIZE, ConfigDef.Type.INT, DEFAULT_PART_SIZE,
                new ConfigDef.Validator() {

                    static final int MAX_BUFFER_SIZE = 2_000_000_000;

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (value == null) {
                            throw new ConfigException(name, null, "Part size must be non-null");
                        }
                        final var number = (Number) value;
                        if (number.longValue() <= 0) {
                            throw new ConfigException(name, value, "Part size must be greater than 0");
                        }
                        if (number.longValue() > MAX_BUFFER_SIZE) {
                            throw new ConfigException(name, value,
                                    "Part size must be no more: " + MAX_BUFFER_SIZE + " bytes (2GB)");
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "The Part Size in S3 Multi-part Uploads in bytes. Maximum is " + Integer.MAX_VALUE
                        + " (2GB) and default is " + DEFAULT_PART_SIZE + " (5MB)",
                GROUP_AWS, 0, ConfigDef.Width.NONE, S3ConfigFragment.AWS_S3_PART_SIZE);

    }

    private static void addDeprecatedTimestampConfig(final ConfigDef configDef) {
        int timestampGroupCounter = 0;

        configDef.define(S3ConfigFragment.TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new TimeZoneValidator(), ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, timestampGroupCounter++, ConfigDef.Width.SHORT, S3ConfigFragment.TIMESTAMP_TIMEZONE);

        configDef.define(S3ConfigFragment.TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new TimestampSourceValidator(), ConfigDef.Importance.LOW,
                "Specifies the the timestamp variable source. Default is wall-clock.", GROUP_FILE,
                timestampGroupCounter, ConfigDef.Width.SHORT, S3ConfigFragment.TIMESTAMP_SOURCE);
    }

    @Override
    public List<ConfigValue> validate(final Map<String, String> props) {
        return super.validate(S3SinkConfig.preprocessProperties(props));
    }


}
