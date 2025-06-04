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

package io.aiven.kafka.connect.config.s3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.aiven.kafka.connect.common.config.FileNameFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This common config handles a specific deprecated date format and is extensible to allow other common configuration
 * that is not specific to a Config Fragment to be available for both sink and source configurations in S3 connectors.
 */

public final class S3CommonConfig {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3CommonConfig.class);

    private S3CommonConfig() {

    }

    public static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        final List<String> keysToProcess = List.of(S3ConfigFragment.AWS_S3_PREFIX_CONFIG,
                FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG);
        if (keysToProcess.stream().noneMatch(properties::containsKey)) {
            return properties;
        }

        final var result = new HashMap<>(properties);
        for (final var prop : keysToProcess) {
            if (properties.containsKey(prop)) {
                String template = properties.get(prop);
                final String originalTemplate = template;

                final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
                template = unitYyyyPattern.matcher(template)
                        .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

                if (!template.equals(originalTemplate)) {
                    LOGGER.warn("{{timestamp:unit=YYYY}} is no longer supported, "
                            + "please use {{timestamp:unit=yyyy}} instead. " + "It was automatically replaced: {}",
                            template);
                }

                result.put(prop, template);
            }
        }
        return result;
    }

}
