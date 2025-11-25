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

    public static class CredTestingDef extends SinkCommonConfig.SinkCommonConfigDef {

        public CredTestingDef() {
            super(null, CompressionType.NONE);
            S3ConfigFragment.update(this, true);
        }
    }
}
