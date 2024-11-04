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

package io.aiven.kafka.connect.common.config.extractors;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;


class SimpleValuePathTest {

    public static Stream<Arguments> ParseDataProvider() {
        return Stream.of(Arguments.of("Path[terms=[a, b, c]]", "a.b.c"), Arguments.of("Path[terms=[a:b:c]]", "a:b:c"),
                Arguments.of("Path[terms=[.b.c]]", ".a.b.c"), Arguments.of("Path[terms=[a.b, c]]", ".:a.b:c"),
                // with some regex special characters
                Arguments.of("Path[terms=[\\a, b, c]]", "\\a.b.c"), Arguments.of("Path[terms=[a.b.c]]", ".\\a.b.c"),
                Arguments.of("Path[terms=[a, b, c]]", ".\\a\\b\\c"),

                Arguments.of("Path[terms=[ [a, b, c]]", " [a.b.c"), Arguments.of("Path[terms=[[a.b.c]]", ". [a.b.c"),
                Arguments.of("Path[terms=[a, b, c]]", ".[a[b[c"),

                Arguments.of("Path[terms=[]]", "."), Arguments.of("Path[terms=[]]", ""),
                Arguments.of("Path[terms=[]]", ".."), Arguments.of("Path[terms=[a]]", "..a"));
    }

    @ParameterizedTest
    @MethodSource("ParseDataProvider")
    void parse(String expected, String toParse) {
        assertEquals(expected, SimpleValuePath.parse(toParse).toString());
    }

    static Schema flatSchema = SchemaBuilder.struct()
            .field("s1", Schema.OPTIONAL_STRING_SCHEMA)
            .field("i1", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    static Schema mapSchema = SchemaBuilder.struct()
            .field("m1",
                    SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    SchemaBuilder.struct()
                                            .field("s2", Schema.OPTIONAL_STRING_SCHEMA)
                                            .field("i2", Schema.OPTIONAL_INT32_SCHEMA)
                                            .optional()
                                            .build())
                            .optional()
                            .build())
            .field("m2", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .build();
    static Schema arraySchema = SchemaBuilder.struct()
            .field("a1", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional().build())
            .field("a2", SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build())
            .field("a3", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .build();

    static SinkRecord toRecord(Schema schema, String json) throws Exception {
        try (JsonDeserializer ds = new JsonDeserializer()) {
            JsonNode jsonValue = ds.deserialize("xx", json.replace('\'', '"').getBytes(StandardCharsets.UTF_8));
            Method m = JsonConverter.class.getDeclaredMethod("convertToConnect", Schema.class, JsonNode.class);
            m.setAccessible(true);
            Object value = m.invoke(null, schema, jsonValue);
            return new SinkRecord("topic", 0, null, null, schema, value, 0);
        }
    }

    static Stream<Arguments> extractDataFromDataProvider() throws Exception {
        return Stream.of(Arguments.of(toRecord(flatSchema, "{'s1': 'hi', 'i1': 42}"), "s1", "hi"),
                Arguments.of(toRecord(flatSchema, "{'s1': 'hi', 'i1': 42}"), "i1", 42),
                Arguments.of(toRecord(flatSchema, "{'s1': 'hi', 'i1': 42}"), "xx", null),

                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m1.k1.i2", 42),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m1.k1.s2", "Hi"),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m1.k1.xx", null),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "mx.k1.i2", null),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m1.k2.s2", "Bi"),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m2.two", 2),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'one':1,'two':2}}"),
                        "m1.one.xx", null),
                Arguments.of(toRecord(mapSchema,
                        "{'m1': {'k1': {'i2': 42, 's2': 'Hi'},'k2': {'i2': 99, 's2': 'Bi'}},'m2':{'with.dot':1}}"),
                        ".:m2:with.dot", 1),

                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a1.0", 1F),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a1.3", 9.9f),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a2.0", 9.0),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a2.1", -1.0),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a3.0", "zero"),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a3.-1", null),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a3.10", null),
                Arguments.of(
                        toRecord(arraySchema, "{'a1': [1,2,17.0,9.9], 'a2':[9,-1,3.14], 'a3':['zero','one','two']}"),
                        "a3.2", "two"));
    }

    @ParameterizedTest
    @MethodSource("extractDataFromDataProvider")
    void extractDataFrom(SinkRecord record, String path, Object expected) {
        final SimpleValuePath underTest = SimpleValuePath.parse(path);
        assertEquals(expected, underTest.extractDataFrom(record));

    }
}
