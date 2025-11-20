/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.iam;

import java.util.Map;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

public class AwsCredentialTestingConfig extends SinkCommonConfig {
    private final S3ConfigFragment s3ConfigFragment;

    public AwsCredentialTestingConfig(final Map<String, String> properties) {
        super(new CredTestingDef(), properties);
        s3ConfigFragment = new S3ConfigFragment(FragmentDataAccess.from(this));
    }

    public static CredTestingDef configDef() { // NOPMD UnusedAssignment
        return new CredTestingDef();
    }

    S3ConfigFragment getS3ConfigFragment() {
        return s3ConfigFragment;
    }

    // private static ConfigDef getBaseConfigDefinition() {
    // final ConfigDef definition = new ConfigDef();
    // addOutputFieldsFormatConfigGroup(definition, OutputFieldType.VALUE);
    // definition.define(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null,
    // ConfigDef.Importance.MEDIUM, "File name template");
    // definition.define(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
    // CompressionType.NONE.name, ConfigDef.Importance.MEDIUM, "File compression");
    // definition.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM,
    // "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
    // + "0 is interpreted as \"unlimited\", which is the default.");
    // return definition;
    // }
    //
    public static class CredTestingDef extends SinkCommonConfig.SinkCommonConfigDef {

        public CredTestingDef() {
            super(null, CompressionType.NONE);
            S3ConfigFragment.update(this, true);
        }
    }

}
