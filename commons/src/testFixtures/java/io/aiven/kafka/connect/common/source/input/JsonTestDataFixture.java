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

package io.aiven.kafka.connect.common.source.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * A testing fixture to generate JSON data.
 */
final public class JsonTestDataFixture {

    private final static String MSG_FORMAT = "{\"id\" : %s, \"message\" : \"%s\", \"value\" : \"value%s\"}%n";

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static DeserializationFeature[] deserializationFeatures = {
            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS };

    static {
        for (DeserializationFeature feature : deserializationFeatures) {
            OBJECT_MAPPER.enable(feature);
        }
        OBJECT_MAPPER.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    private JsonTestDataFixture() {
        // do not instantiate
    }
    /**
     * Creates the specified number of JSON records encoded into a string.
     *
     * @param recordCount
     *            the number of records to generate.
     * @return The specified number of JSON records encoded into a string.
     */
    public static String generateJsonRecs(final int recordCount) {
        return generateJsonRecs(recordCount, "test message");
    }

    /**
     * Generates a single JSON record
     *
     * @param id
     *            the id for the record
     * @param msg
     *            the message for the record
     * @return a standard JSON test record.
     */
    public static String generateJsonRec(final int id, String msg) {
        return String.format(MSG_FORMAT, id, msg, id);
    }

    /**
     * Creates Json test data.
     *
     * @param recordCount
     *            the number of records to create.
     * @param testMessage
     *            the message for the records.
     * @return
     */
    public static String generateJsonRecs(final int recordCount, final String testMessage) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            jsonRecords.append(generateJsonRec(i, testMessage));
        }
        return jsonRecords.toString();
    }

    public static JsonNode readJsonRecord(byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readTree(bytes);
    }

    public static List<JsonNode> readJsonRecords(Collection<String> values) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (String value : values) {
            result.add(OBJECT_MAPPER.readTree(value));
        }
        return result;
    }
}
