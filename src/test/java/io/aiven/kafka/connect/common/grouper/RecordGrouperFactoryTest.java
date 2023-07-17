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

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.templating.Template;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RecordGrouperFactoryTest {

    private static final SinkRecord T0P0R0 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 0);
    private static final SinkRecord T0P0R1 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 1);
    private static final SinkRecord T0P0R2 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2);
    private static final SinkRecord T0P0R3 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 3);
    private static final SinkRecord T0P0R4 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 4);
    private static final SinkRecord T0P0R5 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 5);

    private static final SinkRecord T0P1R0 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null, 10);
    private static final SinkRecord T0P1R1 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 11);
    private static final SinkRecord T0P1R2 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 12);
    private static final SinkRecord T0P1R3 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null, 13);

    private static final SinkRecord T1P1R0 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null, 1000);
    private static final SinkRecord T1P1R1 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null, 1001);
    private static final SinkRecord T1P1R2 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1002);
    private static final SinkRecord T1P1R3 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 1003);

    @Test
    void keyOnly() {
        final Template filenameTemplate = Template.of("{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertEquals(RecordGrouperFactory.KEY_RECORD, grType);
    }

    @Test
    void topicPartitionAndKey() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertEquals(RecordGrouperFactory.TOPIC_PARTITION_KEY_RECORD, grType);
    }

    @Test
    void topicPartition() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{start_offset}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        System.out.println(grType);
        assertEquals(RecordGrouperFactory.TOPIC_PARTITION_RECORD, grType);
    }
}
