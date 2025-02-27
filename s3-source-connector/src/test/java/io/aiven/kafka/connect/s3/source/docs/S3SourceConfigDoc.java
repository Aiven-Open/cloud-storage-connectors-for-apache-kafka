/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.s3.source.docs;

import java.io.IOException;

import io.aiven.kafka.connect.docs.ConfigDocumentation;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.Test;

public class S3SourceConfigDoc {
    @Test
    public void generate() throws IOException {
        ConfigDocumentation.main(new String[] { "-c", S3SourceConfig.class.getName(), "-f", "TEXT", "-o",
                "src/site/markdown/s3-source-connector/S3SourceConfig.txt" });

        ConfigDocumentation.main(new String[] { "-c", S3SourceConfig.class.getName(), "-f", "YAML", "-o",
                "src/site/s3-source-connector/S3SourceConfig.yml" });
    }
}
