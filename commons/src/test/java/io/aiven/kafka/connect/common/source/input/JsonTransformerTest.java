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

package io.aiven.kafka.connect.common.source.input;

import static io.aiven.kafka.connect.common.config.TransformerFragment.SCHEMAS_ENABLE;
import static io.aiven.kafka.connect.common.source.input.Transformer.UNKNOWN_STREAM_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class JsonTransformerTest {

    public static final String TESTTOPIC = "testtopic";
    JsonTransformer jsonTransformer;

    SourceCommonConfig sourceCommonConfig;

    @Mock
    private IOSupplier<InputStream> inputStreamIOSupplierMock;

    JsonConverter jsonConverter;

    @BeforeEach
    void setUp() {
        jsonConverter = new JsonConverter();
        final Map<String, String> config = new HashMap<>();
        config.put(SCHEMAS_ENABLE, "false");
        jsonConverter.configure(config, false);

        jsonTransformer = new JsonTransformer(jsonConverter);
        sourceCommonConfig = mock(SourceCommonConfig.class);
    }

    @AfterEach
    void destroy() {
        jsonConverter.close();
    }

    @Test
    void testHandleValueDataWithValidJson() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                getJsonRecs(100).getBytes(StandardCharsets.UTF_8));

        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            expected.add("value" + i);
        }

        final Stream<SchemaAndValue> records = jsonTransformer.getRecords(() -> validJsonInputStream,
                UNKNOWN_STREAM_LENGTH, TESTTOPIC, 1, sourceCommonConfig, 0);

        assertThat(records).extracting(SchemaAndValue::value)
                .extracting(sv -> ((Map) sv).get("key"))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void testHandleValueDataWithValidJsonSkipFew() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                getJsonRecs(100).getBytes(StandardCharsets.UTF_8));

        final List<String> expected = new ArrayList<>();
        for (int i = 25; i < 100; i++) {
            expected.add("value" + i);
        }

        final Stream<SchemaAndValue> records = jsonTransformer.getRecords(() -> validJsonInputStream,
                UNKNOWN_STREAM_LENGTH, TESTTOPIC, 1, sourceCommonConfig, 25L);

        assertThat(records).extracting(SchemaAndValue::value)
                .extracting(sv -> ((Map) sv).get("key"))
                .containsExactlyElementsOf(expected);

    }

    @Test
    void testHandleValueDataWithInvalidJson() throws IOException {
        final InputStream invalidJsonInputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> invalidJsonInputStream;

        final Stream<SchemaAndValue> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier,
                UNKNOWN_STREAM_LENGTH, TESTTOPIC, 1, sourceCommonConfig, 0);

        assertThat(jsonNodes).isEmpty();

    }

    @Test
    void testGetRecordsWithIOException() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException"));
        final Stream<SchemaAndValue> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, UNKNOWN_STREAM_LENGTH, "topic", 0, null, 0);

        assertThat(resultStream).isEmpty();
    }

    @Test
    void testCustomSpliteratorWithIOExceptionDuringInitialization() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<SchemaAndValue> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, UNKNOWN_STREAM_LENGTH, "topic", 0, null, 0);

        assertThat(resultStream).isEmpty();
    }

    static String getJsonRecs(final int recordCount) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            jsonRecords.append(String.format("{\"key\":\"value%d\"}", i));
            if (i < recordCount) {
                jsonRecords.append("\n"); // NOPMD AppendCharacterWithChar
            }
        }
        return jsonRecords.toString();
    }
}
