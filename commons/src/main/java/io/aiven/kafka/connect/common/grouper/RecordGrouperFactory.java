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

package io.aiven.kafka.connect.common.grouper;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.templating.Template;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

public final class RecordGrouperFactory {

    public static final String KEY_RECORD = KeyRecordGrouper.class.getName();

    public static final String TOPIC_PARTITION_RECORD = TopicPartitionRecordGrouper.class.getName();
    public static final String TOPIC_PARTITION_KEY_RECORD = TopicPartitionKeyRecordGrouper.class.getName();

    public static final String KEY_TOPIC_PARTITION_RECORD = KeyAndTopicPartitionRecordGrouper.class.getName();

    private static final Map<String, List<Pair<String, Boolean>>> SUPPORTED_VARIABLES = new LinkedHashMap<>();

    static {
        SUPPORTED_VARIABLES.put(TOPIC_PARTITION_RECORD,
                List.of(Pair.of(FilenameTemplateVariable.TOPIC.name, true),
                        Pair.of(FilenameTemplateVariable.PARTITION.name, true),
                        Pair.of(FilenameTemplateVariable.START_OFFSET.name, true),
                        Pair.of(FilenameTemplateVariable.TIMESTAMP.name, false)));
        SUPPORTED_VARIABLES.put(TOPIC_PARTITION_KEY_RECORD,
                List.of(Pair.of(FilenameTemplateVariable.TOPIC.name, true),
                        Pair.of(FilenameTemplateVariable.PARTITION.name, true),
                        Pair.of(FilenameTemplateVariable.KEY.name, true),
                        Pair.of(FilenameTemplateVariable.START_OFFSET.name, true),
                        Pair.of(FilenameTemplateVariable.TIMESTAMP.name, false)));
        SUPPORTED_VARIABLES.put(KEY_RECORD, List.of(Pair.of(FilenameTemplateVariable.KEY.name, true)));
        SUPPORTED_VARIABLES.put(KEY_TOPIC_PARTITION_RECORD,
                List.of(Pair.of(FilenameTemplateVariable.KEY.name, true),
                        Pair.of(FilenameTemplateVariable.TOPIC.name, false),
                        Pair.of(FilenameTemplateVariable.PARTITION.name, false)));
    }

    public static final List<String> ALL_SUPPORTED_VARIABLES = SUPPORTED_VARIABLES.values()
            .stream()
            .flatMap(List::stream)
            .map(Pair::getLeft)
            .collect(Collectors.toList());

    private static final Set<String> KEY_RECORD_REQUIRED_VARS = SUPPORTED_VARIABLES.get(KEY_RECORD)
            .stream()
            .filter(Pair::getRight)
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> TOPIC_PARTITION_RECORD_REQUIRED_VARS = SUPPORTED_VARIABLES
            .get(TOPIC_PARTITION_RECORD)
            .stream()
            .filter(Pair::getRight)
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> TOPIC_PARTITION_KEY_RECORD_REQUIRED_VARS = SUPPORTED_VARIABLES
            .get(TOPIC_PARTITION_KEY_RECORD)
            .stream()
            .filter(Pair::getRight)
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> KEY_TOPIC_PARTITION_RECORD_REQUIRED_VARS = SUPPORTED_VARIABLES
            .get(KEY_TOPIC_PARTITION_RECORD)
            .stream()
            .filter(Pair::getRight)
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> TOPIC_PARTITION_RECORD_OPT_VARS = SUPPORTED_VARIABLES.get(TOPIC_PARTITION_RECORD)
            .stream()
            .filter(p -> !p.getRight())
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> TOPIC_PARTITION_KEY_RECORD_OPT_VARS = SUPPORTED_VARIABLES
            .get(TOPIC_PARTITION_KEY_RECORD)
            .stream()
            .filter(p -> !p.getRight())
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    private static final Set<String> KEY_TOPIC_PARTITION_RECORD_OPT_VARS = SUPPORTED_VARIABLES
            .get(KEY_TOPIC_PARTITION_RECORD)
            .stream()
            .filter(p -> !p.getRight())
            .map(Pair::getLeft)
            .collect(Collectors.toSet());

    public static final String SUPPORTED_VARIABLES_LIST = SUPPORTED_VARIABLES.values()
            .stream()
            .map(v -> v.stream().map(Pair::getLeft).collect(Collectors.joining(",")))
            .collect(Collectors.joining("; "));

    private RecordGrouperFactory() {
    }

    public static String resolveRecordGrouperType(final Template template) {
        final Supplier<Set<String>> variables = () -> new HashSet<>(template.variablesSet());
        if (isByTopicPartitionKeyRecord(variables.get())) {
            return TOPIC_PARTITION_KEY_RECORD;
        } else if (isByTopicPartitionRecord(variables.get())) {
            return TOPIC_PARTITION_RECORD;
        } else if (isByKeyRecord(variables.get())) {
            return KEY_RECORD;
        } else if (isByKeyTopicPartitionRecord(variables.get())) {
            return KEY_TOPIC_PARTITION_RECORD;
        } else {
            throw new IllegalArgumentException(String
                    .format("unsupported set of template variables, supported sets are: %s", SUPPORTED_VARIABLES_LIST));
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    public static RecordGrouper newRecordGrouper(final AivenCommonConfig config) {
        final Template fileNameTemplate = config.getFilenameTemplate();
        final boolean isSchemaBased = config.getFormatType() == FormatType.PARQUET
                || config.getFormatType() == FormatType.AVRO;
        if (config.getCustomRecordGrouperBuilder() != null) {
            try {
                final CustomRecordGrouperBuilder builder = config.getCustomRecordGrouperBuilder()
                        .getDeclaredConstructor()
                        .newInstance();

                builder.configure(config);
                builder.setMaxRecordsPerFile(config.getMaxRecordsPerFile());
                builder.setFilenameTemplate(fileNameTemplate);
                builder.setSchemaBased(isSchemaBased);
                builder.setTimestampSource(config.getFilenameTimestampSource());

                return builder.build();
            } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException
                    | InvocationTargetException e) {
                throw new IllegalArgumentException("Failed to create custom record grouper", e);
            }
        }
        final String grType = resolveRecordGrouperType(fileNameTemplate);
        if (KEY_RECORD.equals(grType)) {
            return new KeyRecordGrouper(fileNameTemplate);
        } else if (KEY_TOPIC_PARTITION_RECORD.equals(grType)) {
            return new KeyAndTopicPartitionRecordGrouper(fileNameTemplate);
        } else {
            final Integer maxRecordsPerFile = config.getMaxRecordsPerFile() == 0 ? null : config.getMaxRecordsPerFile();
            if (TOPIC_PARTITION_KEY_RECORD.equals(grType)) {
                return isSchemaBased
                        ? new SchemaBasedTopicPartitionKeyRecordGrouper(fileNameTemplate, maxRecordsPerFile,
                                config.getFilenameTimestampSource())
                        : new TopicPartitionKeyRecordGrouper(fileNameTemplate, maxRecordsPerFile,
                                config.getFilenameTimestampSource());
            } else {
                return isSchemaBased
                        ? new SchemaBasedTopicPartitionRecordGrouper(fileNameTemplate, maxRecordsPerFile,
                                config.getFilenameTimestampSource())
                        : new TopicPartitionRecordGrouper(fileNameTemplate, maxRecordsPerFile,
                                config.getFilenameTimestampSource());
            }
        }
    }

    private static boolean isByKeyRecord(final Set<String> vars) {
        return KEY_RECORD_REQUIRED_VARS.equals(vars);
    }

    private static boolean isByTopicPartitionRecord(final Set<String> vars) {
        final Set<String> requiredVars = Sets.intersection(TOPIC_PARTITION_RECORD_REQUIRED_VARS, vars).immutableCopy();
        vars.removeAll(requiredVars);
        final boolean containsRequiredVars = TOPIC_PARTITION_RECORD_REQUIRED_VARS.equals(requiredVars);
        final boolean containsOptionalVars = vars.isEmpty()
                || !Collections.disjoint(TOPIC_PARTITION_RECORD_OPT_VARS, vars);
        return containsRequiredVars && containsOptionalVars;
    }

    private static boolean isByTopicPartitionKeyRecord(final Set<String> vars) {
        final Set<String> requiredVars = Sets.intersection(TOPIC_PARTITION_KEY_RECORD_REQUIRED_VARS, vars)
                .immutableCopy();
        vars.removeAll(requiredVars);
        final boolean containsRequiredVars = TOPIC_PARTITION_KEY_RECORD_REQUIRED_VARS.equals(requiredVars);
        final boolean containsOptionalVars = vars.isEmpty()
                || !Collections.disjoint(TOPIC_PARTITION_KEY_RECORD_OPT_VARS, vars);
        return containsRequiredVars && containsOptionalVars;
    }

    private static boolean isByKeyTopicPartitionRecord(final Set<String> vars) {
        final Set<String> requiredVars = Sets.intersection(KEY_TOPIC_PARTITION_RECORD_REQUIRED_VARS, vars)
                .immutableCopy();
        vars.removeAll(requiredVars);
        final boolean containsRequiredVars = KEY_TOPIC_PARTITION_RECORD_REQUIRED_VARS.equals(requiredVars);
        final boolean containsOptionalVars = vars.isEmpty()
                || !Collections.disjoint(KEY_TOPIC_PARTITION_RECORD_OPT_VARS, vars);
        return containsRequiredVars && containsOptionalVars;
    }
}
