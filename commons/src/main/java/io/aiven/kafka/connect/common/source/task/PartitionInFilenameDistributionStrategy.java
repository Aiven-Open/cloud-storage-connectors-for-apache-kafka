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

package io.aiven.kafka.connect.common.source.task;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PartitionInFilenameDistributionStrategy} finds a partition in the object's filename by matching it to an
 * expected format, and assigns all partitions to the same task.
 * <p>
 * This useful when a sink connector has created the object name in a format like
 * {@code topicname-{{partition}}-{{start_offset}}}, and we want all objects with the same partition to be processed
 * within a single task.
 */
public final class PartitionInFilenameDistributionStrategy implements ObjectDistributionStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionInFilenameDistributionStrategy.class);
    private final static String NUMBER_REGEX_PATTERN = "(\\d)+";
    // Use a named group to return the partition in a complex string to always get the correct information for the
    // partition number.
    private final static String PARTITION_NAMED_GROUP_REGEX_PATTERN = "(?<partition>\\d)+";
    private final static String PARTITION_PATTERN = "\\{\\{partition}}";
    private final static String START_OFFSET_PATTERN = "\\{\\{start_offset}}";
    private final static String TIMESTAMP_PATTERN = "\\{\\{timestamp}}";
    public static final String PARTITION = "partition";
    private Pattern partitionPattern;

    private int maxTasks;

    PartitionInFilenameDistributionStrategy(final int maxTasks, final String expectedSourceNameFormat) {
        configureDistributionStrategy(maxTasks, expectedSourceNameFormat);
    }

    /**
     *
     * @param sourceNameToBeEvaluated
     *            is the filename/table name of the source for the connector.
     * @return Predicate to confirm if the given source name matches
     */
    @Override
    public boolean isPartOfTask(final int taskId, final String sourceNameToBeEvaluated) {
        if (sourceNameToBeEvaluated == null) {
            LOG.warn("Ignoring as it is not passing a correct filename to be evaluated.");
            return false;
        }
        final Matcher match = partitionPattern.matcher(sourceNameToBeEvaluated);
        if (match.find()) {
            return toBeProcessedByThisTask(taskId, maxTasks, Integer.parseInt(match.group(PARTITION)));
        }
        LOG.warn("Unable to find the partition from this file name {}", sourceNameToBeEvaluated);
        return false;
    }

    /**
     * When a connector reconfiguration event is received this method should be called to ensure the correct strategy is
     * being implemented by the connector.
     *
     * @param maxTasks
     *            maximum number of configured tasks for this connector
     * @param expectedSourceNameFormat
     *            what the format of the source should appear like so to configure the task distribution.
     */
    @Override
    public void reconfigureDistributionStrategy(final int maxTasks, final String expectedSourceNameFormat) {
        configureDistributionStrategy(maxTasks, expectedSourceNameFormat);
    }

    private void configureDistributionStrategy(final int maxTasks, final String expectedSourceNameFormat) {
        if (expectedSourceNameFormat == null || !expectedSourceNameFormat.contains(PARTITION_PATTERN)) {
            throw new ConfigException(String.format(
                    "Source name format %s missing partition pattern {{partition}}, please configure the expected source to include the partition pattern.",
                    expectedSourceNameFormat));
        }
        setMaxTasks(maxTasks);
        // Build REGEX Matcher
        String regexString = StringUtils.replace(expectedSourceNameFormat, START_OFFSET_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, TIMESTAMP_PATTERN, NUMBER_REGEX_PATTERN);
        regexString = StringUtils.replace(regexString, PARTITION_PATTERN, PARTITION_NAMED_GROUP_REGEX_PATTERN);
        try {
            partitionPattern = Pattern.compile(regexString);
        } catch (IllegalArgumentException iae) {
            throw new ConfigException(
                    String.format("Unable to compile the regex pattern %s to retrieve the partition id.", regexString),
                    iae);
        }
    }

    private void setMaxTasks(final int maxTasks) {
        this.maxTasks = maxTasks;
    }

}
