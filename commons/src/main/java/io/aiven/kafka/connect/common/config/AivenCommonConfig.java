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

import java.util.Map;

/**
 * @deprecated Use {@link SinkCommonConfig} instead
 */
@Deprecated
public class AivenCommonConfig extends SinkCommonConfig {
    public static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    public static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";

    protected AivenCommonConfig(final SinkCommonConfigDef definition, final Map<String, String> originals) {
        super(definition, originals);
    }

}
