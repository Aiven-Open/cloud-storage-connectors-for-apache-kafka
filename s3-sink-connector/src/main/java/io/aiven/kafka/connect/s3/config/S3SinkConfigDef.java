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
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;

import io.aiven.commons.collections.Scale;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.ScaleValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.S3OutputStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import static io.aiven.commons.collections.Scale.GiB;

public class S3SinkConfigDef extends SinkCommonConfig.SinkCommonConfigDef {

    private static final String GROUP_AWS = "AWS";
    private static final String GROUP_FILE = "File";

    public S3SinkConfigDef() {
        super(null, CompressionType.GZIP);
        S3ConfigFragment.update(this);
        addS3partSizeConfig(this);
        addDeprecatedTimestampConfig(this);
    }

//    @Override
//    public List<ConfigValue> validate(final Map<String, String> props) {
//        return super.validate(S3SinkConfig.preprocessProperties(props));
//    }

    private static void addDeprecatedTimestampConfig(final ConfigDef configDef) {
        int timestampGroupCounter = 0;

        configDef.define(S3ConfigFragment.TIMESTAMP_TIMEZONE, Type.STRING, ZoneOffset.UTC.toString(),
                new TimeZoneValidator(), Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, ++timestampGroupCounter, ConfigDef.Width.SHORT, S3ConfigFragment.TIMESTAMP_TIMEZONE);

        configDef.define(S3ConfigFragment.TIMESTAMP_SOURCE, Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new TimestampSourceValidator(), Importance.LOW,
                "Specifies the the timestamp variable source. Default is wall-clock.", GROUP_FILE,
                ++timestampGroupCounter, ConfigDef.Width.SHORT, S3ConfigFragment.TIMESTAMP_SOURCE);
    }

    private static void addS3partSizeConfig(final ConfigDef configDef) {

        // add awsS3SinkCounter if more S3 Sink Specific config is added
        // This is used to set orderInGroup
        configDef.define(S3ConfigFragment.AWS_S3_PART_SIZE, Type.INT, S3OutputStream.DEFAULT_PART_SIZE,
                ScaleValidator.between(0, GiB.asBytes(2), Scale.IEC)
                , Importance.MEDIUM,
                "The Part Size in S3 Multi-part Uploads in bytes. Maximum is " + GiB.units(2) + " and default is " + Scale.size(S3OutputStream.DEFAULT_PART_SIZE, Scale.IEC),
                GROUP_AWS, 0, ConfigDef.Width.NONE, S3ConfigFragment.AWS_S3_PART_SIZE);
    }


}
