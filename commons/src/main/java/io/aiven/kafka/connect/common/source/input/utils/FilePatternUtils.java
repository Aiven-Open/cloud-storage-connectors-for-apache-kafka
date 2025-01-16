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

import io.aiven.kafka.connect.common.source.task.Context;

import org.apache.commons.lang3.StringUtils;

public final class FilePatternUtils<K> {
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

    final Pattern pattern;
    final Optional<String> targetTopic;

    public FilePatternUtils(final String pattern, final String targetTopic) {
        this.pattern = configurePattern(pattern);
        this.targetTopic = Optional.ofNullable(targetTopic);
    }

    /**
     *
     * @param expectedSourceNameFormat
     *            This is a string in the expected compatible format which will allow object name or keys to have unique
     *            information such as partition number, topic name, offset and timestamp information.
     * @return A pattern which is configured to allow extraction of the key information from object names and keys.
     */
    private Pattern configurePattern(final String expectedSourceNameFormat) {
        if (expectedSourceNameFormat == null) {
            throw new ConfigException(
                    "Source name format is missing please configure the expected source to include the partition pattern.");
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

    public Optional<Context<K>> process(final K sourceName) {
        if (fileMatches(sourceName.toString())) {
            final Optional<String> topic = getTopic(sourceName.toString());
            final Optional<Integer> partition = getPartitionId(sourceName.toString());
            final Optional<Integer> offset = getOffset(sourceName.toString());
            return Optional
                    .of(new Context<K>(topic.orElse(null), offset.orElse(null), partition.orElse(null), sourceName));
        }
        return Optional.empty();

    }

    private boolean fileMatches(final String sourceName) {
        return matchPattern(sourceName).isPresent();
    }

    private Optional<String> getTopic(final String sourceName) {
        if (targetTopic.isPresent()) {
            return targetTopic;
        }

        return matchPattern(sourceName).flatMap(matcher -> {
            try {
                // TODO check why this worked before without the try catch
                return Optional.of(matcher.group(PATTERN_TOPIC_KEY));
            } catch (IllegalArgumentException ex) {
                // It is possible that when checking for the group it does not match and returns an
                // illegalArgumentException
                return Optional.empty();
            }
        });
    }

    private Optional<Integer> getPartitionId(final String sourceName) {
        return matchPattern(sourceName).flatMap(matcher -> {
            try {
                return Optional.of(Integer.parseInt(matcher.group(PATTERN_PARTITION_KEY)));
            } catch (IllegalArgumentException e) {
                // It is possible that when checking for the group it does not match and returns an
                // illegalStateException, Number format exception is also covered by this in this case.
                return Optional.empty();
            }
        });

    }

    private Optional<Integer> getOffset(final String sourceName) {
        return matchPattern(sourceName).flatMap(matcher -> {
            try {
                return Optional.of(Integer.parseInt(matcher.group(START_OFFSET_PATTERN)));
            } catch (IllegalArgumentException e) {
                // It is possible that when checking for the group it does not match and returns an
                // illegalStateException, Number format exception is also covered by this in this case.
                return Optional.empty();
            }
        });

    }

    private Optional<Matcher> matchPattern(final String sourceName) {
        if (sourceName == null) {
            throw new IllegalArgumentException("filePattern and sourceName must not be null");
        }
        final Matcher matcher = pattern.matcher(sourceName);
        return matcher.find() ? Optional.of(matcher) : Optional.empty();
    }

}
