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

package io.aiven.kafka.connect.common.format;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

/**
 * A testing fixture to generate/read JSON data.
 */
final public class JsonTestDataFixture {

    private final static String MSG_FORMAT = "{\"id\" : %s, \"message\" : \"%s\", \"value\" : \"value%s\"}%n";

    public static final String SCHEMA_JSON = "{\n  \"type\": \"struct\", \"name\": \"TestRecord\",\n "
            + "  \"fields\": [\n {\"field\": \"message\", \"type\": \"string\"},\n"
            + "    {\"field\": \"id\", \"type\": \"int32\"}\n  ]\n}";

    public static final String CONNECT_EXTRA_SCHEMA_JSON = "{\n  \"type\": \"struct\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"name\": \"message\", \"type\": \"string\"},\n"
            + "    {\"name\": \"id\", \"type\": \"int32\"}\n  ],\n"
            + "    \"connect.version\":1, \"connect.name\": \"TestRecord\"}\n";

    public static final String EVOLVED_SCHEMA_JSON = "{\n  \"type\": \"struct\",\n  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n    {\"field\": \"message\", \"type\": \"string\"},\n"
            + "    {\"field\": \"id\", \"type\": \"int32\"},\n"
            + "    {\"field\": \"age\", \"type\": \"int32\", \"default\":0}\n  ]\n}";

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final static DeserializationFeature[] DESERIALIZATION_FEATURES = {
            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS };

    static {
        for (final DeserializationFeature feature : DESERIALIZATION_FEATURES) {
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
     * @param messageId
     *            the id for the record
     * @param msg
     *            the message for the record
     * @return a standard JSON test record.
     */
    public static String generateJsonRec(final int messageId, final String msg) {
        return String.format(MSG_FORMAT, messageId, msg, messageId);
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

    /**
     * Reads a json record from the byte array.
     * @param bytes the bytes to extract the record from.
     * @return JsonNode read from the bytes.
     * @throws IOException on IO error.
     */
    public static JsonNode readJsonRecord(final byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readTree(bytes);
    }

    /**
     * read multiple JSON records.
     * @param values The Strings containing the serialized JSON records.
     * @return a list of JsonRecords extracted from the values.
     * @throws IOException on IO error.
     */
    public static List<JsonNode> readJsonRecords(final Collection<String> values) throws IOException {
        final List<JsonNode> result = new ArrayList<>();
        for (final String value : values) {
            result.add(OBJECT_MAPPER.readTree(value));
        }
        return result;
    }

    /**
     * Reads a list of JsonRecords from an array of bytes.
     * Reads the bytes line by line.
     * @param bytes the serialized json records.
     * @return a list of JsonRecords extracted from the values.
     * @throws IOException on IO error.
     */
    public static List<JsonNode> readJsonRecords(final byte[] bytes) throws IOException {
        final List<JsonNode> result = new ArrayList<>();
        for (final String value : readLines(bytes)) {
            result.add(OBJECT_MAPPER.readTree(value));
        }
        return result;
    }

    /**
     * Reads and decodes CSV based lines.  Reads each line from the byte array and splits it based on ',' returning the
     * field values specified.
     * @param input the serialized data
     * @param fieldsToDecode the indices of the fields to return in the value.
     * @return a list of lists of strings.  The innermost list are the values returned from the csv files.
     * @throws IOException on IO error.
     */
    public static List<List<String>> readAndDecodeLines(final byte[] input, final int... fieldsToDecode)
            throws IOException {
        try (InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(input), StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines()
                    .map(l -> l.split(","))
                    .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                    .collect(Collectors.toList());
        }

    }

    /**
     * Reads based lines from the byte array.
     * @param input the serialized data
     * @return a list of lines from the byte data.
     * @throws IOException on IO error.
     */
    public static List<String> readLines(final byte[] input) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(input), StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines().collect(Collectors.toList());
        }
    }

    /**
     * Decodes base 64 encoded fields.
     * @param originalFields the list of fields.
     * @param fieldsToDecode the list of fields to decode.
     * @return the original fields with the specified fields decoded.
     */
    private static List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    /**
     * Decode a base 64 string.
     * @param value the value to decode.
     * @return the decoded value.
     */
    public static String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
