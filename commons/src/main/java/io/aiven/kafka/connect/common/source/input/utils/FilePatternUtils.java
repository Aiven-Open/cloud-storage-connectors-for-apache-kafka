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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FilePatternUtils allows the construction of a regex pattern to extract the
 * {@link io.aiven.kafka.connect.common.source.task.Context Context} from an Object Key.
 *
 */
public final class FilePatternUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilePatternUtils.class);
    public static final String PATTERN_PARTITION_KEY = "partition";
    public static final String PATTERN_TOPIC_KEY = "topic";
    public static final String PATTERN_START_OFFSET_KEY = "startOffset"; // no undercore allowed as it breaks the regex.
    public static final String START_OFFSET_PATTERN = "{{start_offset}}";
    public static final String TIMESTAMP_PATTERN = "{{timestamp}}";
    public static final String PARTITION_PATTERN = "{{" + PATTERN_PARTITION_KEY + "}}";
    public static final String TOPIC_PATTERN = "{{" + PATTERN_TOPIC_KEY + "}}";

    // Use a named group to return the partition in a complex string to always get the correct information for the
    // partition number.
    public static final String PARTITION_NAMED_GROUP_REGEX_PATTERN = "(?<" + PATTERN_PARTITION_KEY + ">\\d+)";
    public static final String START_OFFSET_NAMED_GROUP_REGEX_PATTERN = "(?<" + PATTERN_START_OFFSET_KEY + ">\\d+)";
    public static final String NUMBER_REGEX_PATTERN = "(?:\\d+)";
    public static final String TOPIC_NAMED_GROUP_REGEX_PATTERN = "(?<" + PATTERN_TOPIC_KEY + ">[a-zA-Z0-9\\-_.]+)";
    public static final String START_OFFSET = "Start offset";

    private final Pattern pattern;
    private final boolean startOffsetConfigured;
    private final boolean partitionConfigured;
    private final boolean topicConfigured;

    /**
     * Creates an instance of FilePatternUtils, this constructor is used to configure the Pattern that is used to
     * extract Context from Objects of type 'K'.
     * @param pattern the file pattern definition.
     * @see #process(Comparable)
     */
    public FilePatternUtils(final String pattern) {
        this.pattern = configurePattern(pattern);
        startOffsetConfigured = pattern.contains(START_OFFSET_PATTERN);
        partitionConfigured = pattern.contains(PARTITION_PATTERN);
        topicConfigured = pattern.contains(TOPIC_PATTERN);
    }

    /**
     * Sets a Regex Pattern based on initial configuration that allows group regex to be used to extract information
     * from the toString() of Object K which is passed in for Context extraction.
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
        String regexString = StringUtils.replace(expectedSourceNameFormat, START_OFFSET_PATTERN,
                START_OFFSET_NAMED_GROUP_REGEX_PATTERN);
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

    /**
     * Creates a Context for the source name.  If the pattern does not match the {@code sourceName.toString()} an empty
     * {@code Optional} is returned.  Otherwise, if "topic", "partition" or "start_offset" are defined in the pattern
     * the values are extracted from {@code sourceName} and returned in the {@code Context}.
     * @param sourceName the Source name to create the context for.
     * @return An {@code Optional Context} for the source name.
     * @param <K> the data type of the source name.
     */
    public <K extends Comparable<K>> Optional<Context<K>> process(final K sourceName) {
        final Optional<Matcher> matcher = fileMatches(sourceName.toString());
        if (matcher.isPresent()) {
            final Context<K> ctx = new Context<>(sourceName);
            getTopic(matcher.get(), sourceName.toString()).ifPresent(ctx::setTopic);
            getPartitionId(matcher.get(), sourceName.toString()).ifPresent(ctx::setPartition);
            getOffset(matcher.get(), sourceName.toString()).ifPresent(ctx::setOffset);
            return Optional.of(ctx);
        }
        LOGGER.debug("{} did not match pattern and was skipped for processing.", sourceName);
        return Optional.empty();

    }

    private Optional<Matcher> fileMatches(final String sourceName) {
        return matchPattern(sourceName);
    }

    private Optional<String> getTopic(final Matcher matcher, final String sourceName) {

        try {
            return Optional.of(matcher.group(PATTERN_TOPIC_KEY));
        } catch (IllegalArgumentException ex) {
            // It is possible that when checking for the group it does not match and returns an
            // illegalArgumentException
            if (topicConfigured) {
                LOGGER.warn("Unable to extract Topic from {} and 'topics' not configured.", sourceName);
            }
            return Optional.empty();
        }

    }

    private Optional<Integer> getPartitionId(final Matcher matcher, final String sourceName) {
        try {
            return Optional.of(Integer.parseInt(matcher.group(PATTERN_PARTITION_KEY)));
        } catch (IllegalArgumentException e) {
            // It is possible that when checking for the group it does not match and returns an
            // illegalStateException, Number format exception is also covered by this in this case.
            if (partitionConfigured) {
                LOGGER.warn("Unable to extract Partition id from {}.", sourceName);
            }
            return Optional.empty();
        }

    }

    private Optional<Long> getOffset(final Matcher matcher, final String sourceName) {
        try {
            return Optional.of(Long.parseLong(matcher.group(PATTERN_START_OFFSET_KEY)));
        } catch (IllegalArgumentException e) {
            // It is possible that when checking for the group it does not match and returns an
            // illegalStateException, Number format exception is also covered by this in this case.
            if (startOffsetConfigured) {
                LOGGER.warn("Unable to extract start offset from {}.", sourceName);
            }
            return Optional.empty();
        }

    }

    private Optional<Matcher> matchPattern(final String sourceName) {
        if (sourceName == null) {
            throw new IllegalArgumentException("filePattern and sourceName must not be null");
        }
        final Matcher matcher = pattern.matcher(sourceName);
        return matcher.find() ? Optional.of(matcher) : Optional.empty();
    }

}
