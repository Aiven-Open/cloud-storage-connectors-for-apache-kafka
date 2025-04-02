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


/**
 * A testing fixture to generate JSON data.
 */
final public class JsonTestDataFixture {

    private final static String msgFormat = "{\"id\" : %s, \"message\" : \"%s\", \"value\" : \"value%s\"}%n";

    /**
     * Creates the specified number of JSON records encoded into a string.
     *
     * @param recordCount
     *            the number of records to generate.
     * @return The specified number of JSON records encoded into a string.
     */
    public static String getJsonRecs(final int recordCount) {
        return getJsonRecs(recordCount, "test message");
    }

    public static String getJsonRecs(final int recordCount, String testMessage) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            jsonRecords.append(String.format(msgFormat, i, testMessage, i));
        }
        return jsonRecords.toString();
    }

}
