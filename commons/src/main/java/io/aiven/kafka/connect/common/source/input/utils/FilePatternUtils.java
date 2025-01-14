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

package io.aiven.kafka.connect.common.source.input.utils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;

import org.apache.commons.lang3.StringUtils;

public final class FilePatternUtils {

    public static final String PATTERN_PARTITION_KEY = "partition";
    public static final String PATTERN_TOPIC_KEY = "topic";
    public static final String START_OFFSET_PATTERN = "{{start_offset}}";
    public static final String TIMESTAMP_PATTERN = "{{timestamp}}";
    public static final String PARTITION_PATTERN = "{{" + PATTERN_PARTITION_KEY + "}}";
    public static final String TOPIC_PATTERN = "{{" + PATTERN_TOPIC_KEY + "}}";

    // Use a named group to return the partition in a complex string to always get the correct information for the
    // partition number.
    public static final String PARTITION_NAMED_GROUP_REGEX_PATTERN = "(?<" + PATTERN_PARTITION_KEY + ">\\d+)";
    public static final String NUMBER_REGEX_PATTERN = "(?:\\d+)";
    public static final String TOPIC_NAMED_GROUP_REGEX_PATTERN = "(?<" + PATTERN_TOPIC_KEY + ">[a-zA-Z0-9\\-_.]+)";

    private FilePatternUtils() {
        // hidden
    }
    public static Pattern configurePattern(final String expectedSourceNameFormat) {
        if (expectedSourceNameFormat == null || !expectedSourceNameFormat.contains(PARTITION_PATTERN)) {
            throw new ConfigException(String.format(
                    "Source name format %s missing partition pattern {{partition}} please configure the expected source to include the partition pattern.",
                    expectedSourceNameFormat));
        }
        // Build REGEX Matcher
        String regexString = StringUtils.replace(expectedSourceNameFormat, START_OFFSET_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, TIMESTAMP_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, TOPIC_PATTERN, TOPIC_NAMED_GROUP_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, PARTITION_PATTERN, PARTITION_NAMED_GROUP_REGEX_PATTERN);
        try {
            return Pattern.compile(regexString);
        } catch (IllegalArgumentException iae) {
            throw new ConfigException(
                    String.format("Unable to compile the regex pattern %s to retrieve the partition id.", regexString),
                    iae);
        }
    }

    public static Optional<String> getTopic(final Pattern filePattern, final String sourceName) {
        return matchPattern(filePattern, sourceName).map(matcher -> matcher.group(PATTERN_TOPIC_KEY));
    }

    public static Optional<Integer> getPartitionId(final Pattern filePattern, final String sourceName) {
        return matchPattern(filePattern, sourceName).flatMap(matcher -> {
            try {
                return Optional.of(Integer.parseInt(matcher.group(PATTERN_PARTITION_KEY)));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        });
    }

    private static Optional<Matcher> matchPattern(final Pattern filePattern, final String sourceName) {
        if (filePattern == null || sourceName == null) {
            throw new IllegalArgumentException("filePattern and sourceName must not be null");
        }

        final Matcher matcher = filePattern.matcher(sourceName);
        return matcher.find() ? Optional.of(matcher) : Optional.empty();
    }

}
